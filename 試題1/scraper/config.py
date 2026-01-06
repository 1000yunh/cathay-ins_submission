"""
Scraper Configuration Module

Contains configuration dataclasses and data models for the RIS scraper.
"""

from dataclasses import dataclass, asdict
from typing import Tuple


@dataclass
class ScraperConfig:
    """
    Scraper configuration settings.

    Attributes:
        BASE_URL: Target URL for scraping
        CITY: Target city (default: 臺北市)
        START_DATE: Query start date in ROC format (YYY-MM-DD)
        END_DATE: Query end date in ROC format
        REGISTER_TYPE: Type of registration to query
        DISTRICTS: Tuple of district names to scrape
        PAGE_LOAD_TIMEOUT: Timeout for page loads (seconds)
        ELEMENT_WAIT_TIMEOUT: Timeout for element waits (seconds)
        ACTION_DELAY: Delay between actions (seconds)
        PAGE_TURN_DELAY: Delay between page turns (seconds)
        MAX_CAPTCHA_RETRIES: Maximum captcha retry attempts
        MAX_PAGE_RETRIES: Maximum page retry attempts
        CAPTCHA_AUTO_OCR: Enable automatic captcha recognition (using ddddocr)
    """

    # Target URL (direct iframe URL, bypassing outer frame)
    BASE_URL: str = "https://www.ris.gov.tw/info-doorplate/app/doorplate/main"

    # Query parameters (as specified in requirements)
    CITY: str = "臺北市"
    START_DATE: str = "114-09-01"
    END_DATE: str = "114-11-30"
    REGISTER_TYPE: str = "門牌初編"

    # Taipei City districts (12 total)
    DISTRICTS: Tuple[str, ...] = (
        "松山區", "信義區", "大安區", "中山區", "中正區", "大同區",
        "萬華區", "文山區", "南港區", "內湖區", "士林區", "北投區"
    )

    # Timeouts and delays
    PAGE_LOAD_TIMEOUT: int = 10
    ELEMENT_WAIT_TIMEOUT: int = 10
    ACTION_DELAY: float = 0.5
    PAGE_TURN_DELAY: float = 1.0

    # Retry settings
    MAX_CAPTCHA_RETRIES: int = 5
    MAX_PAGE_RETRIES: int = 2

    # CAPTCHA settings
    CAPTCHA_AUTO_OCR: bool = True


@dataclass
class AddressRecord:
    """
    Single address registration record.

    Represents one row of scraped data from the RIS website.

    Attributes:
        city: City name (e.g., 臺北市)
        district: District name (e.g., 大安區)
        full_address: Complete address string
        register_date: Registration date in ROC format
        register_type: Type of registration
        raw_data: Raw data string from the page
    """
    city: str
    district: str
    full_address: str
    register_date: str
    register_type: str
    raw_data: str = ""

    def validate(self) -> tuple[bool, str]:
        """
        Validate record data quality.

        Returns:
            tuple: (is_valid, error_message)
        """
        if not self.city:
            return False, "Missing city"
        if not self.district:
            return False, "Missing district"
        if not self.full_address:
            return False, "Missing full_address"
        if not self.register_type:
            return False, "Missing register_type"
        if len(self.full_address) < 5:
            return False, f"Address too short: {self.full_address}"
        return True, ""

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)
