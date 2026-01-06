"""
Data Processing Module

Provides data cleaning, transformation, and parsing functions for RIS scraper data.
Handles:
- Full-width to half-width conversion
- Whitespace normalization
- ROC (Taiwan) date parsing
- Address structure parsing
"""

import unicodedata
import re
from datetime import date
from typing import Optional
from dataclasses import dataclass, field
from enum import Enum


# =============================================================================
# Error Types for Quarantine
# =============================================================================

class ErrorType(str, Enum):
    """Error types for quarantine records."""
    DATE_FORMAT = "DATE_FORMAT"
    MISSING_FIELD = "MISSING_FIELD"
    INVALID_ADDRESS = "INVALID_ADDRESS"
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class AddressParts:
    """Structured address components."""
    city: str                        # 臺北市
    district: str                    # 大安區
    village: Optional[str] = None    # 富台里 or 富台村
    neighborhood: Optional[str] = None  # 019 (from 019鄰)
    road: Optional[str] = None       # 信義路
    section: Optional[str] = None    # 四段
    lane: Optional[str] = None       # 100 (not 100巷)
    alley: Optional[str] = None      # 5 (not 5弄)
    number: Optional[str] = None     # 10 (not 10號)
    floor: Optional[str] = None      # 3 (not 3樓)
    floor_dash: Optional[str] = None # 1 (from 之1)
    raw_address: str = ""            # 原始地址


@dataclass
class ProcessedRecord:
    """Successfully processed record."""
    city: str
    district: str
    full_address: str
    address_parts: AddressParts
    assignment_date: date
    assignment_date_roc: str  # ROC format: 114-11-07
    assignment_type: str
    raw_data: dict = field(default_factory=dict)


@dataclass
class QuarantineRecord:
    """Record that failed validation."""
    raw_data: dict
    error_type: ErrorType
    validation_error: str
    source_url: Optional[str] = None


# =============================================================================
# Text Cleaning Functions
# =============================================================================

def fullwidth_to_halfwidth(text: str) -> str:
    """
    Convert full-width characters to half-width.

    Examples:
        ００８ → 008
        １４７巷 → 147巷
        ＡＢＣ → ABC

    Args:
        text: Input string with potential full-width characters

    Returns:
        String with all full-width converted to half-width
    """
    if not text:
        return text
    return unicodedata.normalize('NFKC', text)


def clean_whitespace(text: str) -> str:
    """
    Remove all whitespace including full-width spaces.

    Handles:
        - Regular spaces
        - Full-width spaces (\\u3000)
        - Tabs, newlines
        - Multiple consecutive spaces

    Args:
        text: Input string with potential whitespace

    Returns:
        String with all whitespace removed
    """
    if not text:
        return text
    # Remove all whitespace characters including full-width space
    return re.sub(r'[\s\u3000]+', '', text)


def clean_text(text: str) -> str:
    """
    Apply all text cleaning operations.

    Pipeline:
        1. Full-width to half-width
        2. Remove whitespace

    Args:
        text: Raw input text

    Returns:
        Cleaned text
    """
    if not text:
        return text
    text = fullwidth_to_halfwidth(text)
    text = clean_whitespace(text)
    return text


# =============================================================================
# Date Parsing Functions
# =============================================================================

# Patterns for ROC date formats
ROC_DATE_PATTERNS = [
    # 民國114年12月30日
    re.compile(r'民國(\d+)年(\d+)月(\d+)日'),
    # 114年12月30日
    re.compile(r'(\d+)年(\d+)月(\d+)日'),
    # 114/12/30
    re.compile(r'(\d+)/(\d+)/(\d+)'),
    # 114-12-30
    re.compile(r'(\d+)-(\d+)-(\d+)'),
]


def parse_roc_date(date_str: str) -> Optional[date]:
    """
    Parse ROC (Taiwan) date to Western date.

    Supports multiple formats:
        - "民國114年12月30日" → date(2025, 12, 30)
        - "114年12月30日" → date(2025, 12, 30)
        - "114/12/30" → date(2025, 12, 30)
        - "114-12-30" → date(2025, 12, 30)

    Args:
        date_str: ROC date string

    Returns:
        Python date object or None if parsing fails
    """
    if not date_str:
        return None

    # Clean the input
    date_str = clean_text(date_str)

    for pattern in ROC_DATE_PATTERNS:
        match = pattern.search(date_str)
        if match:
            try:
                roc_year = int(match.group(1))
                month = int(match.group(2))
                day = int(match.group(3))

                # Validate ROC year range (民國 100-120 年 = 2011-2031)
                if not (100 <= roc_year <= 120):
                    continue

                # Convert ROC year to Western year
                western_year = roc_year + 1911

                # Validate month and day
                if not (1 <= month <= 12):
                    continue
                if not (1 <= day <= 31):
                    continue

                return date(western_year, month, day)

            except ValueError:
                # Invalid date (e.g., Feb 30)
                continue

    return None


def validate_roc_date(date_str: str) -> tuple[bool, str]:
    """
    Validate ROC date format and reasonableness.

    Args:
        date_str: ROC date string

    Returns:
        tuple: (is_valid, error_message)
    """
    if not date_str:
        return False, "Date string is empty"

    parsed = parse_roc_date(date_str)
    if parsed is None:
        return False, f"Cannot parse date: {date_str}"

    # Check if date is not in the future
    if parsed > date.today():
        return False, f"Date is in the future: {parsed}"

    return True, ""


def to_roc_date_string(western_date: date) -> str:
    """
    Convert Western date to ROC date string format.

    Example:
        date(2025, 11, 7) → "114-11-07"

    Args:
        western_date: Python date object

    Returns:
        ROC date string in format "YYY-MM-DD"
    """
    if not western_date:
        return ""

    roc_year = western_date.year - 1911
    return f"{roc_year}-{western_date.month:02d}-{western_date.day:02d}"


# =============================================================================
# Address Parsing Functions
# =============================================================================

# Pattern for parsing Taiwan addresses
# Handles: 臺北市大安區信義路四段100巷5弄10號3樓之1
ADDRESS_PATTERN = re.compile(
    r'^'
    r'(?P<city>[\u4e00-\u9fff]+[市縣])'           # 臺北市
    r'(?P<district>[\u4e00-\u9fff]+[區鄉鎮市])'   # 大安區
    r'(?P<village>[\u4e00-\u9fff]+[里村])?'       # 里/村 (optional)
    r'(?P<neighborhood>\d+鄰)?'                   # 鄰 (optional)
    r'(?P<road>[\u4e00-\u9fff]+[路街道大])?'      # 信義路
    r'(?P<section>[一二三四五六七八九十]+段)?'    # 四段
    r'(?P<lane>\d+)巷'                            # 100巷 (capture 100)
    r'|(?P<lane_only>\d+巷)?'                     # for optional case
)

# Individual patterns for flexible extraction
LANE_PATTERN = re.compile(r'(\d+)巷')
ALLEY_PATTERN = re.compile(r'(\d+)弄')
NUMBER_PATTERN = re.compile(r'(\d+)號')
FLOOR_PATTERN = re.compile(r'([\d一二三四五六七八九十]+)樓')
FLOOR_DASH_PATTERN = re.compile(r'樓之(\d+|[一二三四五六七八九十]+)')

# Chinese number conversion
CHINESE_DIGIT = {
    '零': 0, '一': 1, '二': 2, '三': 3, '四': 4,
    '五': 5, '六': 6, '七': 7, '八': 8, '九': 9, '十': 10,
}

def chinese_to_arabic(chinese: str) -> str:
    """
    Convert Chinese number to Arabic number.

    Examples:
        一 → 1
        十 → 10
        十一 → 11
        二十 → 20
        二十二 → 22
        三十五 → 35
    """
    if not chinese:
        return chinese

    # If already Arabic number, return as-is
    if chinese.isdigit():
        return chinese

    # Handle simple cases
    if len(chinese) == 1:
        return str(CHINESE_DIGIT.get(chinese, chinese))

    # Handle compound numbers (up to 99)
    result = 0

    if '十' in chinese:
        parts = chinese.split('十')
        tens = parts[0]
        ones = parts[1] if len(parts) > 1 else ''

        # Handle tens digit
        if tens == '':
            result = 10  # 十 = 10
        else:
            result = CHINESE_DIGIT.get(tens, 0) * 10  # 二十 = 20

        # Handle ones digit
        if ones:
            result += CHINESE_DIGIT.get(ones, 0)  # 二十二 = 22
    else:
        # Fallback for unknown patterns
        return chinese

    return str(result)


def parse_address(full_address: str) -> Optional[AddressParts]:
    """
    Parse full address into structured components.

    Example:
        Input: "臺北市大安區信義路四段100巷5弄10號3樓之1"
        Output: AddressParts(
            city="臺北市",
            district="大安區",
            road="信義路",
            section="四段",
            lane="100",      # Clean value, no suffix
            alley="5",       # Clean value, no suffix
            number="10",     # Clean value, no suffix
            floor="3",       # Clean value, no suffix
            floor_dash="1"   # From 之1
        )

    Args:
        full_address: Complete address string

    Returns:
        AddressParts object or None if parsing fails
    """
    if not full_address:
        return None

    # Clean the address first
    cleaned = clean_text(full_address)

    # Extract city and district first
    # Use non-greedy +? to avoid matching village names ending with 市
    city_district_pattern = re.compile(
        r'^(?P<city>[\u4e00-\u9fff]+?[市縣])'
        r'(?P<district>[\u4e00-\u9fff]+?[區鄉鎮市])'
    )
    city_match = city_district_pattern.match(cleaned)
    if not city_match:
        return None

    city = city_match.group('city')
    district = city_match.group('district')

    # Extract road and section
    # Road names usually end with 路/街/道 and may be followed by a section (段)
    road = None
    section = None

    # Remove city and district from the beginning for road parsing
    remaining = cleaned[len(city) + len(district):]

    # Extract 里/村 (village) and 鄰 (neighborhood)
    village = None
    neighborhood = None
    village_neigh_pattern = re.compile(r'^([\u4e00-\u9fff]+[里村])?(\d+鄰)?')
    village_neigh_match = village_neigh_pattern.match(remaining)
    if village_neigh_match:
        if village_neigh_match.group(1):
            village = village_neigh_match.group(1)  # e.g., "富台里"
        if village_neigh_match.group(2):
            # Number only, remove leading zeros: "019鄰" → "19"
            neighborhood = village_neigh_match.group(2).replace('鄰', '').lstrip('0') or '0'
        remaining = remaining[village_neigh_match.end():]

    # Extract road and section
    road_section_pattern = re.compile(
        r'^([\u4e00-\u9fff]+[路街道])([一二三四五六七八九十]+段)?'
    )
    road_match = road_section_pattern.match(remaining)
    if road_match:
        road = road_match.group(1)
        section = road_match.group(2)

    # Extract lane (巷) - clean value without suffix
    lane = None
    lane_match = LANE_PATTERN.search(cleaned)
    if lane_match:
        lane = lane_match.group(1)

    # Extract alley (弄) - clean value without suffix
    alley = None
    alley_match = ALLEY_PATTERN.search(cleaned)
    if alley_match:
        alley = alley_match.group(1)

    # Extract number (號) - clean value without suffix
    number = None
    number_match = NUMBER_PATTERN.search(cleaned)
    if number_match:
        number = number_match.group(1)

    # Extract floor (樓) - clean value, convert Chinese to Arabic
    floor = None
    floor_match = FLOOR_PATTERN.search(cleaned)
    if floor_match:
        floor_val = floor_match.group(1)
        floor = chinese_to_arabic(floor_val)

    # Extract floor_dash (之X) - clean value, convert Chinese to Arabic
    floor_dash = None
    floor_dash_match = FLOOR_DASH_PATTERN.search(cleaned)
    if floor_dash_match:
        floor_dash_val = floor_dash_match.group(1)
        floor_dash = chinese_to_arabic(floor_dash_val)

    return AddressParts(
        city=city,
        district=district,
        village=village,
        neighborhood=neighborhood,
        road=road,
        section=section,
        lane=lane,
        alley=alley,
        number=number,
        floor=floor,
        floor_dash=floor_dash,
        raw_address=full_address
    )


def validate_address(full_address: str) -> tuple[bool, str]:
    """
    Validate address format.

    Args:
        full_address: Address string to validate

    Returns:
        tuple: (is_valid, error_message)
    """
    if not full_address:
        return False, "Address is empty"

    if len(full_address) < 5:
        return False, f"Address too short: {full_address}"

    parts = parse_address(full_address)
    if parts is None:
        return False, f"Cannot parse address: {full_address}"

    if not parts.city:
        return False, "Missing city"

    if not parts.district:
        return False, "Missing district"

    return True, ""


# =============================================================================
# Record Processing Pipeline
# =============================================================================

def process_record(raw: dict) -> tuple[Optional[ProcessedRecord], Optional[QuarantineRecord]]:
    """
    Complete data processing pipeline.

    Pipeline steps:
        1. fullwidth_to_halfwidth()
        2. clean_whitespace()
        3. parse_address()
        4. parse_roc_date()
        5. validate()
        6. Success -> (ProcessedRecord, None)
           Failure -> (None, QuarantineRecord)

    Args:
        raw: Raw record dictionary with keys:
            - full_address: Address string
            - register_date: ROC date string
            - register_type: Registration type
            - city: City name (optional, will be parsed from address)
            - district: District name (optional, will be parsed from address)

    Returns:
        tuple: (ProcessedRecord or None, QuarantineRecord or None)
    """
    # Extract fields
    full_address = raw.get('full_address', '')
    register_date = raw.get('register_date', '')
    register_type = raw.get('register_type', '')
    city = raw.get('city', '')
    district = raw.get('district', '')

    # Step 1 & 2: Clean text
    full_address = clean_text(full_address)
    register_date = clean_text(register_date)
    register_type = clean_text(register_type)

    # Step 3: Validate required fields
    if not full_address:
        return None, QuarantineRecord(
            raw_data=raw,
            error_type=ErrorType.MISSING_FIELD,
            validation_error="Missing full_address"
        )

    if not register_type:
        return None, QuarantineRecord(
            raw_data=raw,
            error_type=ErrorType.MISSING_FIELD,
            validation_error="Missing register_type"
        )

    # Step 4: Parse address
    address_parts = parse_address(full_address)
    if address_parts is None:
        return None, QuarantineRecord(
            raw_data=raw,
            error_type=ErrorType.INVALID_ADDRESS,
            validation_error=f"Cannot parse address: {full_address}"
        )

    # Use parsed city/district or fallback to provided values
    final_city = address_parts.city or city
    final_district = address_parts.district or district

    if not final_city or not final_district:
        return None, QuarantineRecord(
            raw_data=raw,
            error_type=ErrorType.MISSING_FIELD,
            validation_error="Missing city or district"
        )

    # Step 5: Parse date
    assignment_date = parse_roc_date(register_date)
    if assignment_date is None:
        return None, QuarantineRecord(
            raw_data=raw,
            error_type=ErrorType.DATE_FORMAT,
            validation_error=f"Cannot parse date: {register_date}"
        )

    # Step 6: All validations passed - create ProcessedRecord
    return ProcessedRecord(
        city=final_city,
        district=final_district,
        full_address=full_address,
        address_parts=address_parts,
        assignment_date=assignment_date,
        assignment_date_roc=to_roc_date_string(assignment_date),
        assignment_type=register_type,
        raw_data=raw
    ), None


# =============================================================================
# Batch Processing
# =============================================================================

def process_records(
    records: list[dict]
) -> tuple[list[ProcessedRecord], list[QuarantineRecord]]:
    """
    Process multiple records.

    Args:
        records: List of raw record dictionaries

    Returns:
        tuple: (list of ProcessedRecords, list of QuarantineRecords)
    """
    processed = []
    quarantined = []

    for raw in records:
        success, failure = process_record(raw)
        if success:
            processed.append(success)
        if failure:
            quarantined.append(failure)

    return processed, quarantined


# =============================================================================
# Testing / Demo
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("Data Processing Module - Demo")
    print("=" * 60)

    # Test full-width conversion
    print("\n1. Full-width to Half-width:")
    test_fw = "００８鄰１４７巷１１弄"
    print(f"   Input:  {test_fw}")
    print(f"   Output: {fullwidth_to_halfwidth(test_fw)}")

    # Test date parsing
    print("\n2. ROC Date Parsing:")
    test_dates = [
        "民國114年12月30日",
        "114年11月7日",
        "114/12/30",
        "114-12-30",
    ]
    for d in test_dates:
        result = parse_roc_date(d)
        print(f"   {d} → {result}")

    # Test address parsing
    print("\n3. Address Parsing:")
    test_addr = "臺北市大安區信義路四段100巷5弄10號3樓之1"
    parts = parse_address(test_addr)
    print(f"   Input: {test_addr}")
    if parts:
        print(f"   City: {parts.city}")
        print(f"   District: {parts.district}")
        print(f"   Road: {parts.road}")
        print(f"   Section: {parts.section}")
        print(f"   Lane: {parts.lane} (clean value)")
        print(f"   Alley: {parts.alley} (clean value)")
        print(f"   Number: {parts.number} (clean value)")
        print(f"   Floor: {parts.floor} (clean value)")
        print(f"   Floor Dash: {parts.floor_dash} (from 之X)")

    # Test full pipeline
    print("\n4. Full Pipeline:")
    test_record = {
        "full_address": "臺北市大安區信義路四段100巷5弄10號七樓之2",
        "register_date": "民國114年11月7日",
        "register_type": "門牌初編",
    }
    success, failure = process_record(test_record)
    if success:
        print(f"   ✓ Processed successfully")
        print(f"     City: {success.city}")
        print(f"     Date (Western): {success.assignment_date}")
        print(f"     Date (ROC): {success.assignment_date_roc}")
        print(f"     Floor: {success.address_parts.floor}")
        print(f"     Floor Dash: {success.address_parts.floor_dash}")
    else:
        print(f"   ✗ Failed: {failure.validation_error}")
