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


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # normalize header strings to snake_case: explicit regex flags for clarity
    df.columns = (
        df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_", regex=False)
            .str.replace("-", "_", regex=False)
            .str.replace("/", "_", regex=False)
            .str.replace(r"[\[\]]", "", regex=True)  # remove brackets
    )
    return df
