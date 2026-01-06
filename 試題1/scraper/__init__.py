"""
RIS Scraper Package

Modular scraper for Taiwan's Department of Household Registration website.
"""

from .config import ScraperConfig, AddressRecord
from .database import DatabaseManager
from .core import RISScraper

__all__ = [
    "ScraperConfig",
    "AddressRecord",
    "DatabaseManager",
    "RISScraper",
]
