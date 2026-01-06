#!/usr/bin/env python3
"""
RIS Scraper Scheduler Service
============================

使用 APScheduler 定時執行爬蟲任務。
採用 subprocess 方式呼叫 main.py，確保每次執行都是乾淨的環境。

Usage:
    # 前景執行 (測試用)
    python scheduler.py

    # 背景執行 (生產環境)
    nohup python scheduler.py > logs/scheduler.log 2>&1 &

Environment Variables:
    SCHEDULER_ENABLED   - 是否啟用排程 (default: true)
    SCHEDULE_CRON       - Cron 表達式 (default: "0 2 * * *" = 每天凌晨 2 點)
    SCHEDULE_DISTRICTS  - 要爬取的區域 (default: "all")
    SCHEDULE_TIMEZONE   - 時區 (default: "Asia/Taipei")
"""

import os
import sys
import subprocess
import signal
import logging
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

# ==========================================
# Configuration
# ==========================================

# Load environment variables
load_dotenv()
load_dotenv("docker/.env", override=True)

# Scheduler settings
SCHEDULER_ENABLED = os.getenv("SCHEDULER_ENABLED", "true").lower() == "true"
SCHEDULE_CRON = os.getenv("SCHEDULE_CRON", "0 2 * * *")  # Default: 2:00 AM daily
SCHEDULE_DISTRICTS = os.getenv("SCHEDULE_DISTRICTS", "all")  # "all" or "大安區,中山區"
SCHEDULE_TIMEZONE = os.getenv("SCHEDULE_TIMEZONE", "Asia/Taipei")

# Paths
BASE_DIR = Path(__file__).parent
MAIN_SCRIPT = BASE_DIR / "main.py"
LOG_DIR = BASE_DIR / "logs"

# ==========================================
# Logging Setup
# ==========================================

def setup_scheduler_logger() -> logging.Logger:
    """設定 Scheduler 專用 Logger"""
    LOG_DIR.mkdir(exist_ok=True)

    logger = logging.getLogger("scheduler")
    logger.setLevel(logging.DEBUG)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(console_format)

    # File handler
    log_file = LOG_DIR / f"scheduler_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(funcName)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_format)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


logger = setup_scheduler_logger()

# ==========================================
# Scraper Job
# ==========================================

def run_scraper_job():
    """
    執行爬蟲任務 (使用 subprocess)

    每次執行都會啟動一個新的 Python 進程，確保：
    - 記憶體完全釋放
    - Chrome/ChromeDriver 進程被清理
    - 錯誤不會影響 scheduler
    """
    job_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger.info(f"[Job {job_id}] Starting scheduled scraper job")
    logger.info(f"[Job {job_id}] Districts: {SCHEDULE_DISTRICTS}")

    start_time = datetime.now()

    try:
        # Build command
        cmd = [sys.executable, str(MAIN_SCRIPT)]

        if SCHEDULE_DISTRICTS.lower() == "all":
            cmd.append("--all-districts")
        else:
            cmd.extend(["--districts", SCHEDULE_DISTRICTS])

        logger.info(f"[Job {job_id}] Executing: {' '.join(cmd)}")

        # Run scraper as subprocess
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(BASE_DIR),
            timeout=7200  # 2 hour timeout
        )

        duration = (datetime.now() - start_time).total_seconds()

        if result.returncode == 0:
            logger.info(f"[Job {job_id}] Completed successfully in {duration:.1f}s")
            # Log last few lines of output
            output_lines = result.stdout.strip().split("\n")[-5:]
            for line in output_lines:
                if line.strip():
                    logger.info(f"[Job {job_id}] > {line}")

            send_notification(
                subject=f"[RIS Scraper] Job {job_id} Completed",
                message=f"Scheduled scraper job completed successfully.\n"
                        f"Duration: {duration:.1f} seconds\n"
                        f"Districts: {SCHEDULE_DISTRICTS}"
            )
        else:
            logger.error(f"[Job {job_id}] Failed with return code {result.returncode}")
            logger.error(f"[Job {job_id}] STDERR: {result.stderr[:500] if result.stderr else 'N/A'}")

            send_notification(
                subject=f"[RIS Scraper] Job {job_id} FAILED",
                message=f"Scheduled scraper job failed!\n"
                        f"Return code: {result.returncode}\n"
                        f"Error: {result.stderr[:500] if result.stderr else 'Unknown'}"
            )

    except subprocess.TimeoutExpired:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"[Job {job_id}] Timeout after {duration:.1f}s")
        send_notification(
            subject=f"[RIS Scraper] Job {job_id} TIMEOUT",
            message=f"Scheduled scraper job timed out after {duration:.1f} seconds."
        )

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.exception(f"[Job {job_id}] Unexpected error: {e}")
        send_notification(
            subject=f"[RIS Scraper] Job {job_id} ERROR",
            message=f"Scheduled scraper job encountered an error:\n{str(e)}"
        )

# ==========================================
# Notification
# ==========================================

def send_notification(subject: str, message: str):
    """發送通知 (整合現有的 alert_service)"""
    try:
        from alert_service import AlertService, Alert, AlertType, AlertSeverity
        alert_service = AlertService()

        # Determine severity based on subject
        if "FAILED" in subject or "ERROR" in subject or "TIMEOUT" in subject:
            severity = AlertSeverity.ERROR
            alert_type = AlertType.SCRAPER_ERROR
        else:
            severity = AlertSeverity.INFO
            alert_type = AlertType.SYSTEM_ERROR  # INFO level for success

        alert = Alert(
            alert_type=alert_type,
            severity=severity,
            title=subject,
            message=message,
            metadata={"source": "scheduler"}
        )
        alert_service.create_alert(alert, send_notification=True)
        logger.debug(f"Notification sent: {subject}")
    except ImportError:
        logger.debug("alert_service not available, skipping notification")
    except Exception as e:
        logger.warning(f"Failed to send notification: {e}")

# ==========================================
# Scheduler Setup
# ==========================================

scheduler_instance = None


def create_scheduler() -> BlockingScheduler:
    """建立並設定 Scheduler"""
    scheduler = BlockingScheduler(timezone=SCHEDULE_TIMEZONE)

    # 使用 APScheduler 內建的 crontab 解析器
    trigger = CronTrigger.from_crontab(SCHEDULE_CRON, timezone=SCHEDULE_TIMEZONE)

    scheduler.add_job(
        run_scraper_job,
        trigger=trigger,
        id="ris_scraper_job",
        name="RIS Address Scraper",
        misfire_grace_time=3600,  # 1 hour grace time
        coalesce=True  # Combine missed runs into one
    )

    return scheduler


def graceful_shutdown(signum, frame):
    """處理 SIGINT/SIGTERM，優雅關閉 Scheduler"""
    logger.info(f"Received signal {signum}, shutting down...")
    if scheduler_instance:
        scheduler_instance.shutdown(wait=False)
    sys.exit(0)

# ==========================================
# Main Entry Point
# ==========================================

def main():
    """主程式入口"""
    global scheduler_instance

    print("=" * 60)
    print("RIS Scraper Scheduler Service")
    print("=" * 60)

    # Check if enabled
    if not SCHEDULER_ENABLED:
        logger.warning("Scheduler is disabled (SCHEDULER_ENABLED=false)")
        print("Scheduler is disabled. Set SCHEDULER_ENABLED=true to enable.")
        return

    # Validate main.py exists
    if not MAIN_SCRIPT.exists():
        logger.error(f"main.py not found at {MAIN_SCRIPT}")
        sys.exit(1)

    # Display configuration
    print(f"\nConfiguration:")
    print(f"  Cron Expression: {SCHEDULE_CRON}")
    print(f"  Timezone: {SCHEDULE_TIMEZONE}")
    print(f"  Districts: {SCHEDULE_DISTRICTS}")

    logger.info(f"Schedule: {SCHEDULE_CRON} ({SCHEDULE_TIMEZONE})")
    logger.info(f"Districts: {SCHEDULE_DISTRICTS}")

    # Register signal handlers
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    # Create scheduler
    try:
        scheduler_instance = create_scheduler()
    except ValueError as e:
        logger.error(f"Invalid cron expression '{SCHEDULE_CRON}': {e}")
        print(f"\nError: Invalid cron expression '{SCHEDULE_CRON}'")
        print("Format: 'minute hour day month day_of_week'")
        print("Example: '0 2 * * *' = Every day at 2:00 AM")
        sys.exit(1)

    # Get next run time
    jobs = scheduler_instance.get_jobs()
    if jobs:
        next_run = jobs[0].trigger.get_next_fire_time(None, datetime.now())
        print(f"\nNext scheduled run: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nScheduler is running. Press Ctrl+C to stop.\n")
    print("=" * 60)

    logger.info("Scheduler started successfully")

    try:
        scheduler_instance.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped by user")
        print("\nScheduler stopped.")


if __name__ == "__main__":
    main()
