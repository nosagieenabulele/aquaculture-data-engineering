"""GoogleSheetExtractor
- Thin wrapper around a gspread client to read worksheets into pandas.DataFrame.
- Uses logging for errors/warnings; returns empty DataFrame on failure.
"""
from typing import List, Optional, Dict
import pandas as pd
import logging

# local re-export that provides the gspread client factory
from .google_client import get_gspread_client

logger = logging.getLogger(__name__)


class GoogleSheetExtractor:
    """
    Extractor that reads worksheets from a Google Spreadsheet.

    Usage:
      extractor = GoogleSheetExtractor(service_account_path="/path/to/creds.json")
      extractor.connect(sheet_url)
      df = extractor.read_worksheet(0)
    """

    def __init__(self, service_account_path: Optional[str] = None):
        self.service_account_path = service_account_path
        self.client = None
        self.spreadsheet = None

    def connect(self, sheet_url: str) -> bool:
        """Authenticate and open the spreadsheet. Returns True on success."""
        try:
            # create client (may raise or return falsy on error)
            self.client = get_gspread_client(self.service_account_path)
            if not self.client:
                logger.error("Could not create gspread client.")
                return False

            # open the spreadsheet by URL
            self.spreadsheet = self.client.open_by_url(sheet_url)
            return True

        except Exception:
            logger.exception("Could not connect to spreadsheet at URL: %s", sheet_url)
            self.spreadsheet = None
            return False

    def read_worksheet(self, worksheet_index: int) -> pd.DataFrame:
        """Read one worksheet (by zero-based index) into a DataFrame.

        Returns an empty DataFrame on failure or if the sheet is empty.
        """
        if not self.spreadsheet:
            logger.error("Spreadsheet not connected. Call connect() first.")
            return pd.DataFrame()

        try:
            # gspread get_worksheet expects zero-based index
            ws = self.spreadsheet.get_worksheet(worksheet_index)
            rows = ws.get_all_values()
            if not rows:
                logger.debug("Worksheet %s is empty.", worksheet_index)
                return pd.DataFrame()

            # Build headers: replace blank headers with Column_<i>
            headers = [
                h if str(h).strip() != "" else f"Column_{i}"
                for i, h in enumerate(rows[0], start=1)
            ]

            # Make duplicate headers unique by appending _<n>
            seen = {}
            normalized = []
            for h in headers:
                if h in seen:
                    seen[h] += 1
                    normalized.append(f"{h}_{seen[h]}")
                else:
                    seen[h] = 0
                    normalized.append(h)

            # Construct DataFrame from remaining rows using normalized headers
            return pd.DataFrame(rows[1:], columns=normalized)

        except Exception:
            logger.exception("Failed to read worksheet index %s.", worksheet_index)
            return pd.DataFrame()

    def read_multiple(self, worksheet_indices: List[int]) -> Dict[str, pd.DataFrame]:
        """Read several worksheets and return a dict keyed by 'sheet_<index>'."""
        # iterate and reuse read_worksheet which already handles errors
        return {f"sheet_{i}": self.read_worksheet(i) for i in worksheet_indices}

    # --- Backwards-compatible wrapper methods used by pipelines ---
    def extract_sheet(self, index: int) -> pd.DataFrame:
        return self.read_worksheet(index)

    def read_sheet(self, index: int) -> pd.DataFrame:
        return self.read_worksheet(index)
