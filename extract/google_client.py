"""Thin re-export for the gspread client factory used by extractors.

Expose get_gspread_client from the repo-local utils package so callers
can import from extract.google_client consistently.
"""

# use repo-local utils module (remove 'etl.' prefix)
from utils.gsheet_client import get_gspread_client

__all__ = ["get_gspread_client"]
