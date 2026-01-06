"""
Loki Logging Configuration

Configures Python logging to send logs to Grafana Loki.
"""

import os
import logging
from logging.handlers import QueueHandler, QueueListener
from queue import Queue

# Try to import Loki handler
try:
    import logging_loki
    from logging_loki import emitter
    LOKI_AVAILABLE = True
except ImportError:
    LOKI_AVAILABLE = False


def setup_loki_logger(
    logger_name: str = "ris_scraper",
    job_name: str = "scraper",
    loki_url: str = None
) -> logging.Logger:
    """
    Setup logger with Loki handler.

    Args:
        logger_name: Name of the logger
        job_name: Job label for Loki (scraper, api, etc.)
        loki_url: Loki push API URL (default: from env or localhost)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(logger_name)

    # Avoid duplicate handlers
    if any(isinstance(h, logging_loki.LokiHandler) for h in logger.handlers if LOKI_AVAILABLE):
        return logger

    # Get Loki URL from environment or use default
    loki_url = loki_url or os.getenv("LOKI_URL", "http://localhost:3100/loki/api/v1/push")

    if not LOKI_AVAILABLE:
        logger.warning("python-logging-loki not installed, Loki logging disabled")
        return logger

    try:
        # Create Loki handler with labels
        loki_handler = logging_loki.LokiHandler(
            url=loki_url,
            tags={"job": job_name},
            version="1",
        )
        loki_handler.setLevel(logging.INFO)

        # Format for Loki
        formatter = logging.Formatter(
            fmt="%(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        loki_handler.setFormatter(formatter)

        # Add handler to logger
        logger.addHandler(loki_handler)
        logger.info(f"Loki logging enabled: {loki_url}")

    except Exception as e:
        logger.warning(f"Failed to setup Loki logging: {e}")

    return logger


def get_loki_handler(job_name: str = "scraper", loki_url: str = None):
    """
    Get a Loki handler for adding to existing loggers.

    Args:
        job_name: Job label for Loki
        loki_url: Loki push API URL

    Returns:
        LokiHandler instance or None if not available
    """
    # Check if Loki is enabled
    loki_enabled = os.getenv("LOKI_ENABLED", "false").lower() == "true"
    if not loki_enabled:
        return None

    if not LOKI_AVAILABLE:
        return None

    loki_url = loki_url or os.getenv("LOKI_URL", "http://localhost:3100/loki/api/v1/push")

    try:
        handler = logging_loki.LokiHandler(
            url=loki_url,
            tags={"job": job_name},
            version="1",
        )
        handler.setLevel(logging.INFO)
        return handler
    except Exception:
        return None
