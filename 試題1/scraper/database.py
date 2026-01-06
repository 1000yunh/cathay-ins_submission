"""
Database Module

Handles all database operations for the RIS scraper.
"""

import os
import re
import logging
from datetime import datetime, timedelta
from typing import List, Optional

import psycopg2
from psycopg2.extras import execute_values, Json
from dotenv import load_dotenv

from .config import AddressRecord

# Try to import ProcessedRecord for processed data saving
try:
    from data_processing import ProcessedRecord
    PROCESSED_RECORD_AVAILABLE = True
except ImportError:
    PROCESSED_RECORD_AVAILABLE = False

# Load environment variables
load_dotenv()

# Get logger
logger = logging.getLogger("ris_scraper")

# Try to import alert service
try:
    from alert_service import alert_service
    ALERT_SERVICE_AVAILABLE = True
except ImportError:
    ALERT_SERVICE_AVAILABLE = False



class DatabaseManager:
    """
    Database connection manager for PostgreSQL.

    Responsibilities:
    - Manage database connections
    - Provide methods for data persistence
    - Handle connection errors gracefully
    """

    def __init__(self, db_url: Optional[str] = None):
        """
        Initialize database connection parameters.

        Args:
            db_url: Database connection URL. If None, uses DATABASE_URL env var.
        """
        self.db_url = db_url or os.getenv(
            "DATABASE_URL",
            "postgresql://a1000yun@localhost:5432/ris_scraper"
        )
        self.conn = None

    def connect(self) -> bool:
        """
        Establish database connection.

        Returns:
            bool: True if connection successful, False otherwise.
        """
        try:
            self.conn = psycopg2.connect(self.db_url)
            logger.info("Database connected successfully")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")

            # Send alert for database error
            if ALERT_SERVICE_AVAILABLE:
                alert_service.database_error(
                    operation="connect",
                    error_message=str(e)
                )

            return False

    def disconnect(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Database disconnected")

    def save_records(self, records: List[AddressRecord]) -> int:
        """
        Save scraped records to database.

        Args:
            records: List of AddressRecord objects

        Returns:
            int: Number of records saved
        """
        if not records:
            logger.warning("No records to save")
            return 0

        if not self.conn:
            if not self.connect():
                return 0

        try:
            cursor = self.conn.cursor()

            # Prepare data for batch insert
            data = []
            for record in records:
                # Parse ROC date
                assignment_date = self._parse_roc_date(record.register_date)

                # Generate ROC date string (114-11-07 format)
                assignment_date_roc = None
                if assignment_date:
                    roc_year = assignment_date.year - 1911
                    assignment_date_roc = f"{roc_year}-{assignment_date.month:02d}-{assignment_date.day:02d}"

                # Build raw_data JSON object
                raw_json = Json({
                    "full_address": record.full_address,
                    "register_date": record.register_date,
                    "register_type": record.register_type,
                    "raw": record.raw_data
                })

                data.append((
                    record.city,
                    record.district,
                    record.full_address,
                    record.register_type,
                    assignment_date,
                    assignment_date_roc,
                    raw_json,
                ))

            # Batch insert (matches Docker PostgreSQL schema)
            insert_query = """
                INSERT INTO house_number_records
                (city, district, full_address, assignment_type, assignment_date,
                 assignment_date_roc, raw_data)
                VALUES %s
                ON CONFLICT DO NOTHING
            """

            execute_values(cursor, insert_query, data)
            self.conn.commit()

            inserted_count = len(data)
            logger.info(f"Successfully saved {inserted_count} records to database")
            return inserted_count

        except Exception as e:
            logger.error(f"Failed to save records: {e}")
            self.conn.rollback()
            return 0
        finally:
            cursor.close()

    def _parse_roc_date(self, roc_date_str: str) -> Optional[datetime]:
        """
        Convert ROC (Taiwan) date to Western date.

        Example: "民國114年11月7日" -> datetime(2025, 11, 7)

        Args:
            roc_date_str: Date string in ROC format

        Returns:
            datetime.date object or None if parsing fails
        """
        try:
            # Remove "民國" prefix
            date_str = roc_date_str.replace("民國", "").strip()

            # Parse year, month, day
            match = re.match(r'(\d+)年(\d+)月(\d+)日', date_str)
            if match:
                roc_year = int(match.group(1))
                month = int(match.group(2))
                day = int(match.group(3))

                # ROC year + 1911 = Western year
                western_year = roc_year + 1911

                return datetime(western_year, month, day).date()
        except Exception as e:
            logger.warning(f"Failed to parse date '{roc_date_str}': {e}")

        return None

    def save_processed_records(self, records: List) -> int:
        """
        Save processed records with parsed address fields to database.

        Args:
            records: List of ProcessedRecord objects

        Returns:
            int: Number of records saved
        """
        if not records:
            logger.warning("No processed records to save")
            return 0

        if not self.conn:
            if not self.connect():
                return 0

        try:
            cursor = self.conn.cursor()

            # Prepare data for batch insert
            data = []
            for record in records:
                # Build raw_data JSON object
                raw_json = Json({
                    "full_address": record.full_address,
                    "assignment_date_roc": record.assignment_date_roc,
                    "assignment_type": record.assignment_type,
                    "original": record.raw_data
                })

                data.append((
                    record.city,
                    record.district,
                    record.full_address,
                    record.address_parts.village,
                    record.address_parts.neighborhood,
                    record.address_parts.road,
                    record.address_parts.section,
                    record.address_parts.lane,
                    record.address_parts.alley,
                    record.address_parts.number,
                    record.address_parts.floor,
                    record.address_parts.floor_dash,
                    record.assignment_type,
                    record.assignment_date,
                    record.assignment_date_roc,
                    raw_json,
                ))

            # Batch insert with all parsed fields
            insert_query = """
                INSERT INTO house_number_records
                (city, district, full_address, village, neighborhood,
                 road, section, lane, alley, number, floor, floor_dash,
                 assignment_type, assignment_date, assignment_date_roc, raw_data)
                VALUES %s
                ON CONFLICT (city, district, full_address, assignment_date)
                DO NOTHING
            """

            execute_values(cursor, insert_query, data)
            self.conn.commit()

            inserted_count = cursor.rowcount if cursor.rowcount > 0 else len(data)
            logger.info(f"Successfully saved {inserted_count} processed records to database")
            return inserted_count

        except Exception as e:
            logger.error(f"Failed to save processed records: {e}")
            self.conn.rollback()
            return 0
        finally:
            cursor.close()

    def log_execution(
        self,
        city: str,
        district: str,
        status: str,
        records_count: int,
        duration: float,
        error_msg: str = None
    ) -> None:
        """
        Log scraper execution status to database.

        Args:
            city: City name
            district: District name
            status: Execution status (SUCCESS, FAILED, PARTIAL)
            records_count: Number of records scraped
            duration: Execution duration in seconds
            error_msg: Error message if failed
        """
        if not self.conn:
            if not self.connect():
                return

        try:
            cursor = self.conn.cursor()

            cursor.execute("""
                INSERT INTO scraper_executions
                (city, district, start_time, end_time, status, records_count,
                 duration_seconds, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                city,
                district,
                datetime.now() - timedelta(seconds=duration),
                datetime.now(),
                status,
                records_count,
                duration,
                error_msg
            ))

            self.conn.commit()
            cursor.close()

        except Exception as e:
            logger.error(f"Failed to log execution: {e}")
