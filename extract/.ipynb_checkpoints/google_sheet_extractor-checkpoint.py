from typing import List, Optional, Dict
import pandas as pd
from .google_client import get_gspread_client

class GoogleSheetExtractor:
    """
    Extractor that reads worksheets from a Google Spreadsheet.
    """

    def __init__(self, service_account_path: Optional[str] = None):
        self.service_account_path = service_account_path
        self.client = None
        self.spreadsheet = None

    def connect(self, sheet_url: str) -> bool:
        """Authenticate and open the spreadsheet. Returns True on success."""
        self.client = get_gspread_client(self.service_account_path)
        if not self.client:
            print("[extractor] ERROR - Could not create gspread client.")
            return False

        try:
            self.spreadsheet = self.client.open_by_url(sheet_url)
            return True
        except Exception as exc:
            print(f"[extractor] ERROR - Could not open spreadsheet: {exc}")
            self.spreadsheet = None
            return False

    def read_worksheet(self, worksheet_index: int) -> pd.DataFrame:
        """Read one worksheet (by zero-based index) into a DataFrame."""
        if not self.spreadsheet:
            print("[extractor] ERROR - Spreadsheet not connected.")
            return pd.DataFrame()
        try:
            ws = self.spreadsheet.get_worksheet(worksheet_index)
            rows = ws.get_all_values()
            if not rows:
                return pd.DataFrame()
            headers = [h if h.strip() != "" else f"Column_{i}" for i, h in enumerate(rows[0], start=1)]
            # Make duplicate headers unique
            seen = {}
            normalized = []
            for h in headers:
                if h in seen:
                    seen[h] += 1
                    normalized.append(f"{h}_{seen[h]}")
                else:
                    seen[h] = 0
                    normalized.append(h)
            return pd.DataFrame(rows[1:], columns=normalized)
        except Exception as exc:
            print(f"[extractor] ERROR - Failed to read worksheet {worksheet_index}: {exc}")
            return pd.DataFrame()

    def read_multiple(self, worksheet_indices: List[int]) -> Dict[str, pd.DataFrame]:
        """Read several worksheets and return a dict keyed by 'sheet_<index>'."""
        return {f"sheet_{i}": self.read_worksheet(i) for i in worksheet_indices}

    # --- Wrapper for existing pipeline calls ---
    def extract_sheet(self, index: int) -> pd.DataFrame:
        return self.read_worksheet(index)

    def read_sheet(self, index: int) -> pd.DataFrame:
        return self.read_worksheet(index)
