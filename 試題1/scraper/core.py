"""
RIS Scraper Core Module

Contains the main RISScraper class with all scraping logic.
"""

import csv
import os
import re
import time
import base64
import logging
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from .config import ScraperConfig, AddressRecord

# Get logger
logger = logging.getLogger("ris_scraper")

# Optional dependencies
try:
    import ddddocr
    DDDDOCR_AVAILABLE = True
except ImportError:
    DDDDOCR_AVAILABLE = False

try:
    from alert_service import alert_service
    ALERT_SERVICE_AVAILABLE = True
except ImportError:
    ALERT_SERVICE_AVAILABLE = False


def log_to_db(level: str, message: str, metadata: dict = None):
    """
    Log message to both file/console and database system_logs table.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        message: Log message
        metadata: Additional metadata as JSON
    """
    # Log to file/console
    log_func = getattr(logger, level.lower(), logger.info)
    log_func(message)

    # Log to database if alert_service is available
    if ALERT_SERVICE_AVAILABLE:
        try:
            alert_service.log_to_db(
                level=level,
                source="scraper",
                message=message,
                metadata=metadata
            )
        except Exception:
            pass  # Don't fail scraping if db logging fails


class RISScraper:
    """
    Scraper for RIS (Household Registration) address data.

    Features:
    - Automatic pagination handling
    - Multi-district scraping
    - Captcha retry mechanism
    - Configurable parameters
    """

    def __init__(self, config: Optional[ScraperConfig] = None):
        """Initialize scraper with configuration."""
        self.config = config or ScraperConfig()
        self.driver: Optional[webdriver.Chrome] = None
        self.results: List[AddressRecord] = []

    # -------------------------------------------------------------------------
    # Browser Management
    # -------------------------------------------------------------------------

    def start_browser(self) -> None:
        """Initialize and start Chrome browser."""
        options = webdriver.ChromeOptions()

        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        self.driver.implicitly_wait(self.config.ELEMENT_WAIT_TIMEOUT)

        log_to_db("INFO", "Browser started successfully")

    def stop_browser(self) -> None:
        """Close browser and cleanup."""
        if self.driver:
            self.driver.quit()
            self.driver = None
            log_to_db("INFO", "Browser closed")

    # -------------------------------------------------------------------------
    # Navigation
    # -------------------------------------------------------------------------

    def go_to_main_page(self) -> None:
        """Navigate to the main query page."""
        logger.info(f"Navigating to: {self.config.BASE_URL}")
        self.driver.get(self.config.BASE_URL)

        WebDriverWait(self.driver, self.config.PAGE_LOAD_TIMEOUT).until(
            EC.presence_of_element_located((By.TAG_NAME, "button"))
        )
        logger.info("Main page loaded")

    def click_date_query_button(self) -> None:
        """Click the 'Query by Date' button."""
        logger.debug("Looking for date query button...")

        try:
            button = self.driver.find_element(
                By.XPATH, "//button[contains(text(), '編訂日期')]"
            )
        except NoSuchElementException:
            buttons = self.driver.find_elements(By.CSS_SELECTOR, "button.btn-info")
            if not buttons:
                raise Exception("Date query button not found")
            button = buttons[0]

        button.click()
        time.sleep(self.config.ACTION_DELAY)
        logger.info("Clicked date query button")

    def select_city(self) -> None:
        """Select city from the map."""
        logger.info(f"Selecting city: {self.config.CITY}")

        WebDriverWait(self.driver, self.config.PAGE_LOAD_TIMEOUT).until(
            EC.presence_of_element_located((By.TAG_NAME, "area"))
        )

        city_area = self.driver.find_element(
            By.XPATH, f"//area[contains(@alt, '{self.config.CITY}')]"
        )
        city_area.click()
        time.sleep(self.config.ACTION_DELAY)
        logger.info(f"Selected {self.config.CITY}")

    # -------------------------------------------------------------------------
    # Dynamic District Fetching
    # -------------------------------------------------------------------------

    def fetch_districts_from_website(self) -> List[str]:
        """
        從 RIS 網站動態抓取當前縣市的行政區列表。

        Returns:
            List[str]: 行政區名稱列表
        """
        logger.info(f"Fetching districts for {self.config.CITY} from website...")

        try:
            # Navigate and select city
            self.go_to_main_page()
            self.click_date_query_button()
            self.select_city()

            # Wait for district dropdown to load
            WebDriverWait(self.driver, self.config.PAGE_LOAD_TIMEOUT).until(
                EC.presence_of_element_located((By.ID, "areaCode"))
            )
            time.sleep(0.5)  # Extra wait for options to populate

            # Extract all district options
            district_select = Select(self.driver.find_element(By.ID, "areaCode"))
            options = district_select.options

            # Filter out empty/placeholder options
            districts = [
                opt.text.strip()
                for opt in options
                if opt.text.strip() and opt.text.strip() != "請選擇"
            ]

            logger.info(f"Found {len(districts)} districts: {districts}")
            return districts

        except Exception as e:
            logger.error(f"Failed to fetch districts from website: {e}")
            # Fallback to config defaults
            logger.warning("Falling back to default districts from config")
            return list(self.config.DISTRICTS)

    def fetch_and_cache_districts(self, cache_file: str = "data/districts_cache.json") -> List[str]:
        """
        抓取行政區並快取到 JSON 檔案。

        Args:
            cache_file: 快取檔案路徑

        Returns:
            List[str]: 行政區名稱列表
        """
        import json
        from pathlib import Path
        from datetime import datetime

        cache_path = Path(cache_file)
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        # Check if cache exists and is recent (within 7 days)
        if cache_path.exists():
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    cache_data = json.load(f)

                city_cache = cache_data.get(self.config.CITY, {})
                if city_cache:
                    cached_date = datetime.fromisoformat(city_cache.get("updated", "2000-01-01"))
                    if (datetime.now() - cached_date).days < 7:
                        logger.info(f"Using cached districts for {self.config.CITY}")
                        return city_cache.get("districts", [])
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logger.warning(f"Cache read error: {e}")

        # Fetch fresh data
        districts = self.fetch_districts_from_website()

        # Update cache
        try:
            cache_data = {}
            if cache_path.exists():
                with open(cache_path, "r", encoding="utf-8") as f:
                    cache_data = json.load(f)

            cache_data[self.config.CITY] = {
                "districts": districts,
                "updated": datetime.now().isoformat()
            }

            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)

            logger.info(f"Districts cached to {cache_file}")
        except Exception as e:
            logger.warning(f"Failed to write cache: {e}")

        return districts

    # -------------------------------------------------------------------------
    # Form Handling
    # -------------------------------------------------------------------------

    def fill_query_form(self, district: str) -> None:
        """Fill the query form with specified parameters."""
        logger.info(f"Filling form for: {district}")

        # Select district
        try:
            district_select = Select(self.driver.find_element(By.ID, "areaCode"))
            district_select.select_by_visible_text(district)
            logger.debug(f"District selected: {district}")
        except NoSuchElementException:
            logger.error("District dropdown not found")

        time.sleep(self.config.ACTION_DELAY)

        # Fill dates using JavaScript
        self.driver.execute_script(
            f"document.getElementById('sDate').value = '{self.config.START_DATE}'"
        )
        self.driver.execute_script(
            f"document.getElementById('eDate').value = '{self.config.END_DATE}'"
        )

        # Select register type
        try:
            type_select = Select(self.driver.find_element(By.ID, "registerKind"))
            type_select.select_by_visible_text(self.config.REGISTER_TYPE)
        except NoSuchElementException:
            logger.error("Register type dropdown not found")

        logger.info("Form filled successfully")

    # -------------------------------------------------------------------------
    # Captcha Handling
    # -------------------------------------------------------------------------

    def handle_captcha(self) -> bool:
        """Handle captcha input with automatic recognition."""
        try:
            captcha_input = self.driver.find_element(By.NAME, "captchaInput")
            captcha_code = None
            max_attempts = 3

            # Method 1: ddddocr
            if DDDDOCR_AVAILABLE and self.config.CAPTCHA_AUTO_OCR:
                logger.info("Using ddddocr for CAPTCHA...")
                for attempt in range(max_attempts):
                    captcha_code = self._recognize_captcha_ocr()
                    if captcha_code:
                        break
                    if attempt < max_attempts - 1:
                        self.refresh_captcha()
                        time.sleep(0.5)

            # Method 2: Manual input
            if not captcha_code:
                logger.info("=" * 50)
                logger.info("CAPTCHA Required - Please check browser")
                logger.info("=" * 50)
                captcha_code = input("Enter captcha >>> ").strip()

            if not captcha_code:
                logger.warning("Captcha cannot be empty")
                return False

            captcha_input.clear()
            captcha_input.send_keys(captcha_code)
            logger.info(f"Captcha entered: {captcha_code}")
            return True

        except NoSuchElementException:
            logger.error("Captcha input field not found")
            return False

    def _recognize_captcha_ocr(self) -> Optional[str]:
        """Recognize captcha using ddddocr OCR."""
        from PIL import Image, ImageOps
        import io

        captcha_selectors = [
            (By.ID, "captchaImage"),
            (By.ID, "captcha"),
            (By.CSS_SELECTOR, "img[alt*='驗證碼']"),
            (By.CSS_SELECTOR, "img[src*='captcha']"),
        ]

        captcha_img = None
        for by, selector in captcha_selectors:
            try:
                captcha_img = self.driver.find_element(by, selector)
                break
            except NoSuchElementException:
                continue

        if not captcha_img:
            logger.warning("Captcha image not found")
            return None

        try:
            img_base64 = captcha_img.screenshot_as_base64
            img_bytes = base64.b64decode(img_base64)

            # Preprocessing
            img = Image.open(io.BytesIO(img_bytes))
            img_gray = ImageOps.grayscale(img)
            img_contrast = ImageOps.autocontrast(img_gray)
            threshold = 128
            img_binary = img_contrast.point(lambda x: 255 if x > threshold else 0)

            buffer = io.BytesIO()
            img_binary.save(buffer, format='PNG')
            processed_bytes = buffer.getvalue()

            ocr = ddddocr.DdddOcr(show_ad=False)
            result = ocr.classification(processed_bytes)
            result = result.strip().upper().replace(" ", "")

            if not result or len(result) != 5:
                result = ocr.classification(img_bytes)
                result = result.strip().upper().replace(" ", "")

            if result and len(result) == 5:
                logger.info(f"OCR recognized: {result}")
                return result

            return None

        except Exception as e:
            logger.warning(f"OCR recognition failed: {e}")
            return None

    def check_captcha_error(self) -> bool:
        """Check if captcha validation failed."""
        # Wait longer for the response (error dialog or result table)
        time.sleep(2.5)
        page_source = self.driver.page_source

        # Check for SweetAlert2 error dialog FIRST (error takes priority)
        has_error_text = "圖形驗證碼驗證失敗" in page_source
        # Also check if swal2-popup is visible (style contains "display: flex" or similar)
        has_swal_visible = 'class="swal2-popup' in page_source and 'style="display: none"' not in page_source.split('swal2-popup')[1][:100] if 'class="swal2-popup' in page_source else False

        logger.debug(f"check_captcha_error: error_text={has_error_text}, swal_visible={has_swal_visible}")

        if has_error_text:
            logger.warning("Captcha error detected - will retry")
            # Try multiple methods to close the SweetAlert2 dialog
            try:
                # Method 1: Click confirm button (確定)
                confirm_btn = self.driver.find_element(By.CSS_SELECTOR, ".swal2-confirm")
                confirm_btn.click()
                time.sleep(0.5)
            except:
                try:
                    # Method 2: Click any visible button in swal2-actions
                    actions = self.driver.find_element(By.CSS_SELECTOR, ".swal2-actions")
                    buttons = actions.find_elements(By.TAG_NAME, "button")
                    for btn in buttons:
                        if btn.is_displayed():
                            btn.click()
                            time.sleep(0.5)
                            break
                except:
                    try:
                        # Method 3: Use JavaScript to close
                        self.driver.execute_script("Swal.close();")
                        time.sleep(0.5)
                    except:
                        pass
            return True

        try:
            alert = self.driver.switch_to.alert
            alert.accept()
            return True
        except:
            pass

        return False

    def refresh_captcha(self) -> bool:
        """Refresh captcha image."""
        try:
            refresh_btn = self.driver.find_element(
                By.XPATH, "//button[contains(text(), '產製新驗證碼')]"
            )
            refresh_btn.click()
            time.sleep(self.config.ACTION_DELAY)
            logger.info("Captcha refreshed")
            return True
        except NoSuchElementException:
            return False

    def submit_with_captcha_retry(self) -> bool:
        """Submit form with captcha retry mechanism."""
        for attempt in range(self.config.MAX_CAPTCHA_RETRIES):
            if attempt > 0:
                cooldown = 3 + attempt * 2
                logger.info(f"Captcha retry {attempt + 1}/{self.config.MAX_CAPTCHA_RETRIES} (cooldown {cooldown}s)")
                time.sleep(cooldown)
                self.refresh_captcha()

            if not self.handle_captcha():
                continue

            self._click_search_button()
            time.sleep(self.config.ACTION_DELAY)

            if self.check_captcha_error():
                continue

            logger.info("Captcha verified successfully")
            return True

        logger.error(f"Max captcha retries ({self.config.MAX_CAPTCHA_RETRIES}) reached")
        time.sleep(10)
        return False

    def _click_search_button(self) -> None:
        """Click the search button."""
        search_btn = self.driver.find_element(
            By.XPATH, "//button[contains(text(), '搜尋')]"
        )
        search_btn.click()
        logger.debug("Search submitted")

    # -------------------------------------------------------------------------
    # Result Parsing
    # -------------------------------------------------------------------------

    def parse_current_page(self, district: str) -> List[AddressRecord]:
        """Parse results from current page."""
        records = []

        try:
            WebDriverWait(self.driver, self.config.PAGE_LOAD_TIMEOUT).until(
                EC.presence_of_element_located((By.ID, "jQGrid"))
            )

            table = self.driver.find_element(By.ID, "jQGrid")
            rows = table.find_elements(By.TAG_NAME, "tr")

            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) < 4:
                    continue

                full_address = cols[1].text.strip()
                register_date = cols[2].text.strip()
                register_type = cols[3].text.strip()

                if not full_address:
                    continue

                record = AddressRecord(
                    city=self.config.CITY,
                    district=district,
                    full_address=full_address,
                    register_date=register_date,
                    register_type=register_type,
                    raw_data=f"{full_address}|{register_date}|{register_type}"
                )

                is_valid, error_msg = record.validate()
                if is_valid:
                    records.append(record)
                else:
                    logger.warning(f"Invalid record: {error_msg}")

        except TimeoutException:
            logger.warning("No results table found")
        except Exception as e:
            logger.error(f"Error parsing results: {e}")

        return records

    def get_pagination_info(self) -> tuple:
        """Get pagination information."""
        try:
            pager_info = self.driver.find_element(By.CLASS_NAME, "ui-paging-info")
            info_text = pager_info.text

            match = re.search(r'共\s*(\d+)\s*條', info_text)
            total_records = int(match.group(1)) if match else 0

            page_input = self.driver.find_element(By.CSS_SELECTOR, "input.ui-pg-input")
            current_page = int(page_input.get_attribute("value") or 1)

            total_pages = (total_records + 49) // 50

            return current_page, total_pages, total_records

        except Exception as e:
            logger.warning(f"Could not get pagination info: {e}")
            return 1, 1, 0

    def go_to_next_page(self) -> bool:
        """Navigate to next page of results."""
        try:
            next_btn = None
            selectors = [
                (By.CSS_SELECTOR, "td[title='Next Page']"),
                (By.CSS_SELECTOR, "[title='Next Page']"),
                (By.CSS_SELECTOR, "td#next_jQGrid"),
            ]

            for by, selector in selectors:
                try:
                    next_btn = self.driver.find_element(by, selector)
                    if next_btn:
                        break
                except NoSuchElementException:
                    continue

            if not next_btn:
                return False

            class_attr = next_btn.get_attribute("class") or ""
            if "ui-state-disabled" in class_attr:
                return False

            next_btn.click()
            time.sleep(self.config.PAGE_TURN_DELAY)

            WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located((By.ID, "jQGrid"))
            )

            return True

        except Exception as e:
            logger.error(f"Error navigating to next page: {e}")
            return False

    def scrape_all_pages(self, district: str) -> List[AddressRecord]:
        """Scrape all pages for a district."""
        all_records = []

        current_page, total_pages, total_records = self.get_pagination_info()
        log_to_db("INFO", f"Found {total_records} records across {total_pages} pages", {"district": district, "total_records": total_records, "total_pages": total_pages})

        if total_records == 0:
            logger.warning("No data found for this query (查無資料)")
            log_to_db("WARNING", f"No data found for {district} (查無資料)", {"district": district, "query_params": "check date range and category"})
            return all_records

        while True:
            page_records = self.parse_current_page(district)
            all_records.extend(page_records)
            logger.info(f"Page {current_page}/{total_pages}: {len(page_records)} records")

            if current_page >= total_pages:
                break

            if not self.go_to_next_page():
                break

            current_page += 1
            time.sleep(0.5)

        log_to_db("INFO", f"Finished scraping {district}: {len(all_records)} total records", {"district": district, "total": len(all_records)})
        return all_records

    # -------------------------------------------------------------------------
    # Main Scraping Logic
    # -------------------------------------------------------------------------

    def scrape_district(self, district: str) -> List[AddressRecord]:
        """Scrape all data for a single district."""
        log_to_db("INFO", f"Scraping: {self.config.CITY} {district}", {"city": self.config.CITY, "district": district})

        try:
            self.go_to_main_page()
            self.click_date_query_button()
            self.select_city()
            self.fill_query_form(district)

            if not self.submit_with_captcha_retry():
                logger.error(f"Failed to verify captcha for {district}")
                return []

            time.sleep(2)
            records = self.scrape_all_pages(district)

            log_to_db("INFO", f"{district}: {len(records)} records collected", {"district": district, "count": len(records)})
            return records

        except Exception as e:
            log_to_db("ERROR", f"Error scraping {district}: {e}", {"district": district, "error": str(e)})

            if ALERT_SERVICE_AVAILABLE:
                alert_service.scraper_error(
                    district=district,
                    error_message=str(e),
                    metadata={"city": self.config.CITY, "district": district}
                )

            return []

    def scrape_all_districts(self) -> List[AddressRecord]:
        """Scrape all districts in Taipei City."""
        log_to_db("INFO", f"Starting full scrape: {len(self.config.DISTRICTS)} districts", {"districts": list(self.config.DISTRICTS)})

        all_records = []

        for district in self.config.DISTRICTS:
            records = self.scrape_district(district)
            all_records.extend(records)

        self.results = all_records
        return all_records

    def run(self, districts: Optional[List[str]] = None) -> List[AddressRecord]:
        """
        Run the scraper.

        Args:
            districts: Optional list of specific districts to scrape.

        Returns:
            List of AddressRecord objects
        """
        try:
            self.start_browser()

            if districts:
                all_records = []
                for district in districts:
                    records = self.scrape_district(district)
                    all_records.extend(records)
                self.results = all_records
            else:
                self.results = self.scrape_all_districts()

            return self.results

        finally:
            self.stop_browser()

    def save_to_csv(
        self,
        records: Optional[List[AddressRecord]] = None,
        output_dir: str = "data",
        filename_prefix: Optional[str] = None
    ) -> str:
        """
        Save records to CSV file.

        Args:
            records: List of records to save. If None, uses self.results
            output_dir: Directory to save CSV file
            filename_prefix: Custom filename prefix (e.g., "raw_addresses_20250101")

        Returns:
            str: Path to saved CSV file
        """
        records = records or self.results

        if not records:
            logger.warning("No records to save")
            return ""

        # Create output directory if not exists
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Generate filename
        if filename_prefix:
            filename = output_path / f"{filename_prefix}.csv"
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = output_path / f"addresses_{timestamp}.csv"

        # Define CSV columns
        fieldnames = [
            "city",
            "district",
            "full_address",
            "register_date",
            "register_type",
            "raw_data"
        ]

        # Write CSV with UTF-8 BOM for Excel compatibility
        with open(filename, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for record in records:
                writer.writerow(asdict(record))

        logger.info(f"Saved {len(records)} records to {filename}")
        return str(filename)
