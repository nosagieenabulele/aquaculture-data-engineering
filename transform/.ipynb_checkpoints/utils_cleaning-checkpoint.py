# etl/transform/utils_cleaning.py
"""
Shared cleaning utilities for all ETL transformers.

Covers:
- Numeric cleaning
- String cleaning
- Temperature cleaning
- Date cleaning (mandatory)
- Column normalization
"""

import pandas as pd
import numpy as np


def clean_numeric_columns(df: pd.DataFrame, columns):
    for col in columns:
        if col not in df.columns:
            continue
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
        df[col].replace({"nan": None, "none": None, "": None}, inplace=True)
    return df


def clean_temperature_column(df: pd.DataFrame, column):
    if column not in df.columns:
        return df
    df[column] = (
        df[column]
        .astype(str)
        .str.replace("°c", "", regex=False)
        .str.replace("c", "", regex=False)
        .str.strip()
        .str.extract(r"(\d+\.?\d*)", expand=False)
    )
    df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def clean_date_column(df: pd.DataFrame, column):
    """
    Standard date cleaner used across all loaders/transformers.
    Converts to datetime, handles blank, malformed, Excel serials.
    """
    if column not in df.columns:
        return df

    def _parse_date(x):
        if pd.isna(x):
            return None
        s = str(x).strip()

        # Excel serial date
        if s.isdigit() and int(s) > 40000:
            return pd.to_datetime("1899-12-30") + pd.to_timedelta(int(s), "D")

        try:
            return pd.to_datetime(s, dayfirst=True, errors="coerce")
        except Exception:
            return None

    df[column] = df[column].apply(_parse_date)
    return df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.replace("-", "_")
            .str.replace("/", "_")
            .str.replace(r"[\[\]]", "", regex=True)  # remove brackets
    )
    return df
