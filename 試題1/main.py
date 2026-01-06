"""
RIS Address Scraper - Main Entry Point

Modular scraper for Taiwan's Department of Household Registration website.
"""

import argparse
import csv
import logging
import time
from datetime import datetime
from pathlib import Path

# Import from modular scraper package
from scraper import RISScraper, DatabaseManager, ScraperConfig

# Import data processing module
try:
    from data_processing import (
        process_records,
        ProcessedRecord,
        QuarantineRecord,
    )
    DATA_PROCESSING_AVAILABLE = True
except ImportError:
    DATA_PROCESSING_AVAILABLE = False


# =============================================================================
# District Helper
# =============================================================================

def get_districts_for_city(city: str, scraper: RISScraper) -> list:
    """
    取得指定縣市的行政區列表。
    優先讀取快取，若無則從網站動態抓取。

    Args:
        city: 縣市名稱
        scraper: RISScraper 實例

    Returns:
        行政區名稱列表
    """
    import json
    from datetime import datetime as dt

    cache_file = Path("data/districts_cache.json")

    # 1. Check cache first
    if cache_file.exists():
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cache_data = json.load(f)

            city_cache = cache_data.get(city, {})
            if city_cache:
                cached_date = dt.fromisoformat(city_cache.get("updated", "2000-01-01"))
                if (dt.now() - cached_date).days < 7:
                    districts = city_cache.get("districts", [])
                    print(f"使用快取的 {city} 行政區 ({len(districts)} 區)")
                    return districts
        except (json.JSONDecodeError, KeyError, ValueError):
            pass

    # 2. No cache or expired - fetch from website
    print(f"從網站抓取 {city} 的行政區...")
    try:
        scraper.start_browser()
        districts = scraper.fetch_and_cache_districts()
        return districts
    except Exception as e:
        print(f"抓取失敗: {e}，使用預設行政區")
        return list(ScraperConfig.DISTRICTS)


# =============================================================================
# Logging Setup
# =============================================================================

# Try to import Loki logger
try:
    from loki_logger import get_loki_handler
    LOKI_AVAILABLE = True
except ImportError:
    LOKI_AVAILABLE = False


def setup_logger(name: str = "ris_scraper", log_dir: str = "logs") -> logging.Logger:
    """
    Configure and return a logger instance.

    Args:
        name: Logger name
        log_dir: Directory for log files

    Returns:
        Configured logger instance
    """
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler (INFO and above)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (DEBUG and above)
    timestamp = datetime.now().strftime("%Y%m%d")
    file_handler = logging.FileHandler(
        log_path / f"scraper_{timestamp}.log",
        encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Loki handler (send logs to Grafana Loki)
    if LOKI_AVAILABLE:
        loki_handler = get_loki_handler(job_name="scraper")
        if loki_handler:
            loki_handler.setLevel(logging.INFO)
            logger.addHandler(loki_handler)
            logger.debug("Loki logging enabled")

    return logger


# Initialize logger
logger = setup_logger()


# =============================================================================
# CSV Helper Functions
# =============================================================================

def save_cleaned_csv(records: list, timestamp: str, output_dir: str = "data") -> str:
    """
    Save cleaned/processed records to CSV.

    Args:
        records: List of ProcessedRecord objects
        timestamp: Timestamp string for filename
        output_dir: Output directory

    Returns:
        Path to saved CSV file
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    filename = output_path / f"cleaned_addresses_{timestamp}.csv"

    fieldnames = [
        "city", "district", "full_address",
        "village", "neighborhood",
        "road", "section", "lane", "alley", "number", "floor", "floor_dash",
        "assignment_date", "assignment_date_roc", "assignment_type",
    ]

    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for rec in records:
            writer.writerow({
                "city": rec.city,
                "district": rec.district,
                "full_address": rec.full_address,
                "village": rec.address_parts.village or "",
                "neighborhood": rec.address_parts.neighborhood or "",
                "road": rec.address_parts.road or "",
                "section": rec.address_parts.section or "",
                "lane": rec.address_parts.lane or "",
                "alley": rec.address_parts.alley or "",
                "number": rec.address_parts.number or "",
                "floor": rec.address_parts.floor or "",
                "floor_dash": rec.address_parts.floor_dash or "",
                "assignment_date": str(rec.assignment_date),
                "assignment_date_roc": rec.assignment_date_roc,
                "assignment_type": rec.assignment_type,
            })

    return str(filename)


def save_quarantine_csv(records: list, timestamp: str, output_dir: str = "data") -> str:
    """
    Save quarantined (failed) records to CSV.

    Args:
        records: List of QuarantineRecord objects
        timestamp: Timestamp string for filename
        output_dir: Output directory

    Returns:
        Path to saved CSV file
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    filename = output_path / f"quarantine_{timestamp}.csv"

    fieldnames = [
        "error_type",
        "validation_error",
        "full_address",
        "register_date",
        "register_type",
    ]

    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for rec in records:
            writer.writerow({
                "error_type": rec.error_type.value,
                "validation_error": rec.validation_error,
                "full_address": rec.raw_data.get("full_address", ""),
                "register_date": rec.raw_data.get("register_date", ""),
                "register_type": rec.raw_data.get("register_type", ""),
            })

    return str(filename)


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    """
    Main function to run the scraper.

    Workflow:
    1. Parse command line arguments
    2. Start browser
    3. Scrape data from specified districts
    4. Save to CSV file (file-first approach)
    5. Process data (clean, validate, parse)
    6. Write to PostgreSQL database
    7. Log execution status

    Command line options:
        --all-districts     Scrape all districts for the city
        --districts "A,B"   Scrape specific districts (comma-separated)
        --fetch-districts   Fetch and display available districts from website
        --city "縣市"       Target city (default: 臺北市)
        --start-date        Start date in ROC format (default: 114-09-01)
        --end-date          End date in ROC format (default: 114-11-30)
        (no args)           Scrape only 大安區 (default)
    """
    parser = argparse.ArgumentParser(description="RIS Address Scraper")
    parser.add_argument(
        "--all-districts",
        action="store_true",
        help="Scrape all districts for the city"
    )
    parser.add_argument(
        "--districts",
        type=str,
        help="Comma-separated list of districts to scrape (e.g., '中正區,大同區')"
    )
    parser.add_argument(
        "--fetch-districts",
        action="store_true",
        help="Fetch and display available districts from website (no scraping)"
    )
    parser.add_argument(
        "--city",
        type=str,
        default="臺北市",
        help="Target city (default: 臺北市)"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="114-09-01",
        help="Start date in ROC format YYY-MM-DD (default: 114-09-01)"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default="114-11-30",
        help="End date in ROC format YYY-MM-DD (default: 114-11-30)"
    )
    parser.add_argument(
        "--register-type",
        type=str,
        default="門牌初編",
        help="Register type: 門牌初編, 門牌增編, 門牌改編, 門牌廢編 (default: 門牌初編)"
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("RIS Address Scraper - Starting")
    logger.info("=" * 60)

    # Create config with specified parameters
    config = ScraperConfig()
    if args.city:
        config.CITY = args.city
    if args.start_date:
        config.START_DATE = args.start_date
    if args.end_date:
        config.END_DATE = args.end_date
    if args.register_type:
        config.REGISTER_TYPE = args.register_type

    logger.info(f"City: {config.CITY}")
    logger.info(f"Date range: {config.START_DATE} ~ {config.END_DATE}")
    logger.info(f"Register type: {config.REGISTER_TYPE}")

    # Create scraper with config
    scraper = RISScraper(config=config)

    # Handle --fetch-districts: only fetch and display districts, then exit
    if args.fetch_districts:
        logger.info(f"Fetching districts for: {config.CITY}")
        print(f"\n正在從網站抓取 {config.CITY} 的行政區...")

        try:
            scraper.start_browser()
            districts = scraper.fetch_and_cache_districts()

            print(f"\n{config.CITY} 共有 {len(districts)} 個行政區：")
            print("-" * 40)
            for i, d in enumerate(districts, 1):
                print(f"  {i:2}. {d}")
            print("-" * 40)
            print(f"\n快取已儲存至: data/districts_cache.json")

        finally:
            scraper.stop_browser()

        return  # Exit without scraping

    start_time = time.time()

    # Initialize database connection
    db = DatabaseManager()

    # Determine target districts
    if args.all_districts:
        # Use dynamic districts: check cache first, then fetch from website
        target_districts = get_districts_for_city(config.CITY, scraper)
        logger.info(f"Mode: All districts for {config.CITY} ({len(target_districts)} districts)")
    elif args.districts:
        target_districts = [d.strip() for d in args.districts.split(",")]
        logger.info(f"Mode: Selected districts: {target_districts}")
    else:
        target_districts = ["大安區"]
        logger.info("Mode: Default (大安區 only)")

    # Run scraper
    results = scraper.run(districts=target_districts)

    # Calculate execution duration
    duration = time.time() - start_time

    # ==== Step 1: Save RAW CSV (file-first, backup) ====
    raw_csv_file = ""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if results:
        raw_csv_file = scraper.save_to_csv(
            filename_prefix=f"raw_addresses_{timestamp}"
        )
        logger.info(f"Raw CSV saved to: {raw_csv_file}")

    # ==== Step 2: Process data (clean, validate, parse) ====
    cleaned_csv_file = ""
    quarantine_csv_file = ""
    processed_records = []
    quarantined_records = []

    if results and DATA_PROCESSING_AVAILABLE:
        logger.info("Processing data...")

        # Convert AddressRecord to dict for processing
        raw_dicts = [
            {
                "full_address": r.full_address,
                "register_date": r.register_date,
                "register_type": r.register_type,
                "city": r.city,
                "district": r.district,
            }
            for r in results
        ]

        # Process records
        processed_records, quarantined_records = process_records(raw_dicts)

        logger.info(f"Processed: {len(processed_records)} success, {len(quarantined_records)} quarantined")

        # Save cleaned CSV
        if processed_records:
            cleaned_csv_file = save_cleaned_csv(processed_records, timestamp)
            logger.info(f"Cleaned CSV saved to: {cleaned_csv_file}")

        # Save quarantine CSV
        if quarantined_records:
            quarantine_csv_file = save_quarantine_csv(quarantined_records, timestamp)
            logger.info(f"Quarantine CSV saved to: {quarantine_csv_file}")

    elif results:
        logger.warning("data_processing module not available, skipping processing")

    # ==== Step 3: Write to database (processed data) ====
    db_saved_count = 0
    if processed_records:
        logger.info("Saving processed records to database...")
        db_saved_count = db.save_processed_records(processed_records)
        logger.info(f"Database saved: {db_saved_count} processed records")
    elif results:
        # Fallback to raw records if processing is not available
        logger.warning("No processed records, saving raw records to database...")
        db_saved_count = db.save_records(results)
        logger.info(f"Database saved: {db_saved_count} raw records")

    # ==== Step 4: Log execution status to database ====
    for district in target_districts:
        district_records = [r for r in results if r.district == district]
        status = "SUCCESS" if district_records else "PARTIAL"

        db.log_execution(
            city=scraper.config.CITY,
            district=district,
            status=status,
            records_count=len(district_records),
            duration=duration
        )

    # Close database connection
    db.disconnect()

    # ==== Summary ====
    logger.info("=" * 60)
    if len(results) == 0:
        logger.warning("Scraping Complete - No data found (查無資料)")
    else:
        logger.info("Scraping Complete!")
    logger.info(f"  Total scraped:    {len(results)}")
    logger.info(f"  Processed:        {len(processed_records)}")
    logger.info(f"  Quarantined:      {len(quarantined_records)}")
    logger.info(f"  Database saved:   {db_saved_count}")
    logger.info(f"  Duration:         {duration:.1f} seconds")
    logger.info("")
    logger.info("  Files:")
    logger.info(f"    Raw CSV:        {raw_csv_file}")
    if cleaned_csv_file:
        logger.info(f"    Cleaned CSV:    {cleaned_csv_file}")
    if quarantine_csv_file:
        logger.info(f"    Quarantine CSV: {quarantine_csv_file}")
    logger.info("=" * 60)

    # Show sample results in log
    if results:
        logger.debug("Sample records:")
        for i, record in enumerate(results[:5], 1):
            logger.debug(f"  {i}. {record.full_address} | {record.register_date}")


if __name__ == "__main__":
    main()
