# etl/transform/utils_cleaning.py
"""
Shared cleaning utilities for all ETL transformers.

Covers:
- Numeric cleaning
- String cleaning
- Temperature cleaning
- Date cleaning (robust handling of Excel serials)
- Column normalization
"""

import pandas as pd
import numpy as np


def clean_numeric_columns(df: pd.DataFrame, columns):
    for col in columns:
        if col not in df.columns:
            continue
        # remove thousands separators, capture first numeric token, coerce to numeric
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.extract(r"([-+]?\d*\.?\d+)", expand=False)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def clean_string_columns(df: pd.DataFrame, columns):
    for col in columns:
        if col not in df.columns:
            continue
        df[col] = df[col].astype(str).str.strip().str.lower()
        # normalize common null tokens to None
        df[col].replace({"nan": None, "none": None, "": None}, inplace=True)
    return df


def clean_temperature_column(df: pd.DataFrame, column):
    if column not in df.columns:
        return df
    df[column] = (
    df[column]
    .astype(str)
    .str.extract(r'(\d+\.?\d*)', expand=False)
)
    df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def clean_date_column(df: pd.DataFrame, column):
    """
    Standard date cleaner used across all loaders/transformers.
    - Handles pandas-friendly datetimes
    - Converts Excel serials > ~40000 to timestamps
    - Returns pd.NaT for unparsable values (keeps datetime dtype)
    """
    if column not in df.columns:
        return df

    def _parse_date(x):
        if pd.isna(x):
            return pd.NaT
        # handle numeric types (int/float) which may be Excel serials
        if isinstance(x, (int, float)):
            try:
                if int(x) > 40000:
                    return pd.to_datetime("1899-12-30") + pd.to_timedelta(int(x), "D")
            except Exception:
                return pd.NaT
        s = str(x).strip()
        if s == "":
            return pd.NaT
        # numeric-looking string that represents an Excel serial
        if s.isdigit() and int(s) > 40000:
            return pd.to_datetime("1899-12-30") + pd.to_timedelta(int(s), "D")
        # fallback to pandas parser (dayfirst preserved)
        parsed = pd.to_datetime(s, dayfirst=True, errors="coerce")
        return parsed if not pd.isna(parsed) else pd.NaT

    df[column] = df[column].apply(_parse_date)
    # ensure column is datetime dtype
    df[column] = pd.to_datetime(df[column], errors="coerce")
    return df

def normalize_columns(df: pd.DataFrame, scan_limit: int = 5) -> pd.DataFrame:
    """
    Normalize column names in a DataFrame.
    - Scans the first `scan_limit` rows to find the best header row
    - Drops rows above the chosen header
    - Normalizes column names to snake_case
    """
    # 1. Define your keywords
    keywords = ['timestamp', 'date', 'category', 'record_date', 'feed_size', 'feed_type', 'amount', 'quantity', 'cost', 'week', 'manufacturer', 'notes', 'feed size']
    
    print(f"Original shape: {df.shape}")
    
    # 2. Score the first X rows based on keyword density
    scores = []
    for i in range(min(scan_limit, len(df))):
        row_str = df.iloc[i].astype(str).str.lower().values
        # Count how many unique keywords appear in this row
        match_count = sum(1 for k in keywords if any(k in cell for cell in row_str))
        scores.append(match_count)

    # Determine the best header row
    max_score = max(scores) if scores else 0
    
    # --- NEW FALLBACK LOGIC ---
    if max_score == 0:
        print("No suitable header row found; using original columns.")
        # We do nothing here; we keep the current df and its current columns
    else:
        header_idx = scores.index(max_score)
        print(f"Chosen header at index: {header_idx} (Score: {max_score})")

        # 3. Drop rows above the chosen header
        df = df.iloc[header_idx:].reset_index(drop=True)
        
        # Set the first row of this slice as columns
        df.columns = df.iloc[0]
        df = df.iloc[1:].reset_index(drop=True)

        # 4. Check the 3 rows AFTER the new header for "junk" duplicate headers
        rows_to_drop = []
        for i in range(min(3, len(df))):
            row_str = df.iloc[i].astype(str).str.lower().values
            if any(k in cell for k in keywords for cell in row_str):
                rows_to_drop.append(i)
        
        if rows_to_drop:
            print(f"Dropping duplicate header rows at indices: {rows_to_drop}")
            df = df.drop(index=rows_to_drop).reset_index(drop=True)

    # 5. Normalize names (snake_case) - Runs whether we found a header or used the original
    df.columns = (
        df.columns.astype(str).str.strip().str.lower()
        .str.replace(r'[ \-/]', '_', regex=True)
        .str.replace(r'[\[\]]', '', regex=True)
    )

    print(f"Final shape: {df.shape}")
    return df
