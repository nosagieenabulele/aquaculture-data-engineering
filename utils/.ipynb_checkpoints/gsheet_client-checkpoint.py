# utils/gsheet_client.py
"""
Google Sheets client helper for ETL extraction.

Creates an authenticated gspread client using a
Google Service Account JSON key.

Version-1 logging style for simplicity (prints).
"""

import os
from typing import Optional

import gspread
from google.oauth2.service_account import Credentials


def get_gspread_client(service_account_path: Optional[str] = None) -> Optional[gspread.Client]:
    """
    Create and return an authorized Google Sheets client.

    Parameters
    ----------
    service_account_path : str, optional
        Path to Google service account JSON.
        If None, loads from GOOGLE_CREDENTIALS_PATH env variable.

    Returns
    -------
    gspread.Client or None
    """
    path = service_account_path or os.getenv("GOOGLE_CREDENTIALS_PATH")

    if not path or not os.path.exists(path):
        print("[gsheet_client] ERROR – Service account JSON not found.")
        return None

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

    try:
        creds = Credentials.from_service_account_file(path, scopes=scopes)
        client = gspread.authorize(creds)
        print("[gsheet_client] Authenticated Google Sheets client.")
        return client
    except Exception as exc:
        print(f"[gsheet_client] ERROR – Failed to authenticate client: {exc}")
        return None
