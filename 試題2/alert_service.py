"""
Alert Service Module

Provides alert notification functionality for the RIS Scraper system.
Records alerts to database and optionally sends notifications via Email.

This implements "試題3: 異常通報" requirements.

Notification Channels:
- Email (SMTP): Enterprise-grade notification for critical alerts
- Database: All alerts are stored in PostgreSQL for audit trail
"""

import os
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum

import psycopg2
from psycopg2.extras import RealDictCursor, Json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logger
logger = logging.getLogger("alert_service")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# =============================================================================
# Enums and Constants
# =============================================================================

class AlertType(str, Enum):
    """Alert types matching database schema."""
    SCRAPER_ERROR = "SCRAPER_ERROR"
    API_EMPTY_RESULT = "API_EMPTY_RESULT"
    DATABASE_ERROR = "DATABASE_ERROR"
    SYSTEM_ERROR = "SYSTEM_ERROR"


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class AlertStatus(str, Enum):
    """Alert status."""
    PENDING = "PENDING"
    SENT = "SENT"
    FAILED = "FAILED"


# =============================================================================
# Email Configuration
# =============================================================================

class EmailConfig:
    """Email configuration from environment variables."""
    SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USER = os.getenv("SMTP_USER", "")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
    SMTP_FROM = os.getenv("SMTP_FROM", "")
    SMTP_TO = os.getenv("SMTP_TO", "").split(",")  # Comma-separated recipients
    SMTP_ENABLED = os.getenv("SMTP_ENABLED", "false").lower() == "true"


# =============================================================================
# Alert Data Class
# =============================================================================

@dataclass
class Alert:
    """Represents an alert notification."""
    alert_type: AlertType
    severity: AlertSeverity
    title: str
    message: str
    metadata: Optional[Dict[str, Any]] = None
    notification_channels: Optional[List[str]] = None


# =============================================================================
# Alert Service
# =============================================================================

class AlertService:
    """
    Service for managing alerts and notifications.

    Features:
    - Record alerts to database
    - Query alert history
    - Send email notifications (Gmail SMTP)
    - Log to system_logs table
    """

    def __init__(self):
        """Initialize alert service."""
        self.db_url = os.getenv(
            "DATABASE_URL",
            "postgresql://a1000yun@localhost:5432/ris_scraper"
        )
        self.conn = None

    def _get_connection(self):
        """Get database connection."""
        if not self.conn or self.conn.closed:
            self.conn = psycopg2.connect(
                self.db_url,
                cursor_factory=RealDictCursor
            )
        return self.conn

    def _close_connection(self):
        """Close database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()
            self.conn = None

    # -------------------------------------------------------------------------
    # Email Notification
    # -------------------------------------------------------------------------

    def _send_email(self, alert: Alert) -> bool:
        """
        Send email notification for an alert.

        Args:
            alert: Alert object to notify about

        Returns:
            True if email sent successfully, False otherwise
        """
        if not EmailConfig.SMTP_ENABLED:
            logger.debug("Email notifications disabled")
            return False

        if not EmailConfig.SMTP_USER or not EmailConfig.SMTP_TO:
            logger.warning("Email configuration incomplete, skipping notification")
            return False

        try:
            # Create email message
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"[{alert.severity.value}] {alert.title}"
            msg["From"] = EmailConfig.SMTP_FROM or EmailConfig.SMTP_USER
            msg["To"] = ", ".join(EmailConfig.SMTP_TO)

            # Plain text body
            text_body = f"""
RIS Scraper System Alert
========================

Severity: {alert.severity.value}
Type: {alert.alert_type.value}
Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Title: {alert.title}

Message:
{alert.message}

Metadata:
{alert.metadata if alert.metadata else 'None'}
"""

            # HTML body
            severity_color = {
                "INFO": "#17a2b8",
                "WARNING": "#ffc107",
                "ERROR": "#dc3545",
                "CRITICAL": "#721c24"
            }.get(alert.severity.value, "#6c757d")

            html_body = f"""
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; }}
        .header {{ background-color: {severity_color}; color: white; padding: 15px; }}
        .content {{ padding: 20px; }}
        .metadata {{ background-color: #f8f9fa; padding: 10px; margin-top: 15px; }}
    </style>
</head>
<body>
    <div class="header">
        <h2>[{alert.severity.value}] {alert.title}</h2>
    </div>
    <div class="content">
        <p><strong>Type:</strong> {alert.alert_type.value}</p>
        <p><strong>Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><strong>Message:</strong></p>
        <p>{alert.message}</p>
        <div class="metadata">
            <strong>Metadata:</strong>
            <pre>{alert.metadata if alert.metadata else 'None'}</pre>
        </div>
    </div>
</body>
</html>
"""

            msg.attach(MIMEText(text_body, "plain"))
            msg.attach(MIMEText(html_body, "html"))

            # Send email (use SSL for port 465, TLS for port 587)
            if EmailConfig.SMTP_PORT == 465:
                with smtplib.SMTP_SSL(EmailConfig.SMTP_HOST, EmailConfig.SMTP_PORT) as server:
                    server.login(EmailConfig.SMTP_USER, EmailConfig.SMTP_PASSWORD)
                    server.sendmail(
                        EmailConfig.SMTP_FROM or EmailConfig.SMTP_USER,
                        EmailConfig.SMTP_TO,
                        msg.as_string()
                    )
            else:
                with smtplib.SMTP(EmailConfig.SMTP_HOST, EmailConfig.SMTP_PORT) as server:
                    server.starttls()
                    server.login(EmailConfig.SMTP_USER, EmailConfig.SMTP_PASSWORD)
                    server.sendmail(
                        EmailConfig.SMTP_FROM or EmailConfig.SMTP_USER,
                        EmailConfig.SMTP_TO,
                        msg.as_string()
                    )

            logger.info(f"Email notification sent for alert: {alert.title}")
            return True

        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
            return False

    # -------------------------------------------------------------------------
    # Core Alert Functions
    # -------------------------------------------------------------------------

    def create_alert(self, alert: Alert, send_notification: bool = True) -> Optional[int]:
        """
        Create and record an alert to database.
        Automatically sends email notification for ERROR/CRITICAL severity.

        Args:
            alert: Alert object containing alert details
            send_notification: Whether to send email notification

        Returns:
            Alert ID if successful, None otherwise
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Determine notification channels
            channels = alert.notification_channels or []
            if send_notification and alert.severity in [AlertSeverity.ERROR, AlertSeverity.CRITICAL]:
                if "email" not in channels:
                    channels.append("email")

            # Initial status
            status = AlertStatus.PENDING.value

            cursor.execute("""
                INSERT INTO alert_notifications
                (alert_type, severity, title, message, metadata,
                 notification_channels, status, sent_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                alert.alert_type.value,
                alert.severity.value,
                alert.title,
                alert.message,
                Json(alert.metadata) if alert.metadata else None,
                channels if channels else None,
                status,
                datetime.now()
            ))

            alert_id = cursor.fetchone()["id"]
            conn.commit()
            cursor.close()

            logger.info(f"Alert created: [{alert.severity.value}] {alert.title}")

            # Send email notification for ERROR/CRITICAL
            if send_notification and alert.severity in [AlertSeverity.ERROR, AlertSeverity.CRITICAL]:
                email_sent = self._send_email(alert)
                self._update_alert_status(
                    alert_id,
                    AlertStatus.SENT if email_sent else AlertStatus.FAILED
                )

            # Also log to system_logs
            self.log_to_db(
                level=alert.severity.value,
                source="alert",
                message=f"{alert.title}: {alert.message}",
                metadata=alert.metadata
            )

            return alert_id

        except Exception as e:
            logger.error(f"Failed to create alert: {e}")
            if self.conn:
                self.conn.rollback()
            return None

    def _update_alert_status(self, alert_id: int, status: AlertStatus) -> bool:
        """Update alert status after notification attempt."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE alert_notifications
                SET status = %s
                WHERE id = %s
            """, (status.value, alert_id))

            conn.commit()
            cursor.close()
            return True

        except Exception as e:
            logger.error(f"Failed to update alert status: {e}")
            return False

    def log_to_db(
        self,
        level: str,
        source: str,
        message: str,
        metadata: Optional[Dict] = None
    ) -> Optional[int]:
        """
        Log message to system_logs table.

        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            source: Source of the log (scraper, api, alert, scheduler)
            message: Log message
            metadata: Optional metadata dict

        Returns:
            Log ID if successful, None otherwise
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO system_logs (level, source, message, metadata)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (level, source, message, Json(metadata) if metadata else None))

            log_id = cursor.fetchone()["id"]
            conn.commit()
            cursor.close()
            return log_id

        except Exception as e:
            logger.error(f"Failed to log to database: {e}")
            if self.conn:
                self.conn.rollback()
            return None

    def get_alerts(
        self,
        limit: int = 50,
        alert_type: Optional[str] = None,
        severity: Optional[str] = None
    ) -> List[Dict]:
        """
        Get alerts from database.

        Args:
            limit: Maximum number of alerts to return
            alert_type: Filter by alert type
            severity: Filter by severity

        Returns:
            List of alert records
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            query = "SELECT * FROM alert_notifications WHERE 1=1"
            params = []

            if alert_type:
                query += " AND alert_type = %s"
                params.append(alert_type)

            if severity:
                query += " AND severity = %s"
                params.append(severity)

            query += " ORDER BY sent_at DESC LIMIT %s"
            params.append(limit)

            cursor.execute(query, params)
            alerts = cursor.fetchall()
            cursor.close()

            return [dict(a) for a in alerts]

        except Exception as e:
            logger.error(f"Failed to get alerts: {e}")
            return []

    def get_alert_stats(self) -> Dict:
        """
        Get alert statistics.

        Returns:
            Dictionary with alert statistics
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Total count
            cursor.execute("SELECT COUNT(*) as total FROM alert_notifications")
            total = cursor.fetchone()["total"]

            # By type
            cursor.execute("""
                SELECT alert_type, COUNT(*) as count
                FROM alert_notifications
                GROUP BY alert_type
            """)
            by_type = {row["alert_type"]: row["count"] for row in cursor.fetchall()}

            # By severity
            cursor.execute("""
                SELECT severity, COUNT(*) as count
                FROM alert_notifications
                GROUP BY severity
            """)
            by_severity = {row["severity"]: row["count"] for row in cursor.fetchall()}

            # Recent (last 24 hours)
            cursor.execute("""
                SELECT COUNT(*) as count
                FROM alert_notifications
                WHERE sent_at > NOW() - INTERVAL '24 hours'
            """)
            recent = cursor.fetchone()["count"]

            cursor.close()

            return {
                "total": total,
                "by_type": by_type,
                "by_severity": by_severity,
                "last_24_hours": recent
            }

        except Exception as e:
            logger.error(f"Failed to get alert stats: {e}")
            return {}

    # -------------------------------------------------------------------------
    # Convenience Methods
    # -------------------------------------------------------------------------

    def scraper_error(
        self,
        district: str,
        error_message: str,
        metadata: Optional[Dict] = None
    ) -> Optional[int]:
        """
        Create a scraper error alert.

        Args:
            district: District where error occurred
            error_message: Error description
            metadata: Additional error details
        """
        alert = Alert(
            alert_type=AlertType.SCRAPER_ERROR,
            severity=AlertSeverity.ERROR,
            title=f"Scraper Error - {district}",
            message=f"Error while scraping {district}: {error_message}",
            metadata=metadata or {"district": district, "error": error_message}
        )
        return self.create_alert(alert)

    def api_error(
        self,
        endpoint: str,
        error_message: str,
        metadata: Optional[Dict] = None
    ) -> Optional[int]:
        """
        Create an API error alert.

        Args:
            endpoint: API endpoint where error occurred
            error_message: Error description
            metadata: Additional error details
        """
        alert = Alert(
            alert_type=AlertType.SYSTEM_ERROR,
            severity=AlertSeverity.ERROR,
            title=f"API Error - {endpoint}",
            message=f"Error at {endpoint}: {error_message}",
            metadata=metadata or {"endpoint": endpoint, "error": error_message}
        )
        return self.create_alert(alert)

    def database_error(
        self,
        operation: str,
        error_message: str,
        metadata: Optional[Dict] = None
    ) -> Optional[int]:
        """
        Create a database error alert.

        Args:
            operation: Database operation that failed
            error_message: Error description
            metadata: Additional error details
        """
        alert = Alert(
            alert_type=AlertType.DATABASE_ERROR,
            severity=AlertSeverity.CRITICAL,
            title=f"Database Error - {operation}",
            message=f"Database {operation} failed: {error_message}",
            metadata=metadata or {"operation": operation, "error": error_message}
        )
        return self.create_alert(alert)

    def api_empty_result(
        self,
        city: str,
        district: str,
        metadata: Optional[Dict] = None
    ) -> Optional[int]:
        """
        Create an API empty result alert.

        Args:
            city: City queried
            district: District queried
            metadata: Additional details
        """
        alert = Alert(
            alert_type=AlertType.API_EMPTY_RESULT,
            severity=AlertSeverity.WARNING,
            title=f"Empty Result - {city} {district}",
            message=f"Query for {city} {district} returned no results",
            metadata=metadata or {"city": city, "district": district}
        )
        return self.create_alert(alert)


# =============================================================================
# Global Instance
# =============================================================================

# Create a global alert service instance
alert_service = AlertService()


# =============================================================================
# Main (Testing)
# =============================================================================

if __name__ == "__main__":
    print("Testing Alert Service...")
    print("=" * 50)

    # Test creating alerts
    alert_service.scraper_error(
        district="大安區",
        error_message="Connection timeout",
        metadata={"retry_count": 3}
    )

    alert_service.api_empty_result(
        city="臺北市",
        district="松山區"
    )

    # Get stats
    stats = alert_service.get_alert_stats()
    print(f"\nAlert Statistics: {stats}")

    # Get recent alerts
    alerts = alert_service.get_alerts(limit=5)
    print(f"\nRecent Alerts ({len(alerts)}):")
    for a in alerts:
        print(f"  [{a['severity']}] {a['title']}")
