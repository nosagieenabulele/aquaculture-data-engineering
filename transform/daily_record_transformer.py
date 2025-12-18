"""
Transforms the raw Daily Record sheet into validated, cleaned data.

Behavior:
 - Normalize headers to snake_case
 - Parse date columns (Excel serials supported)
 - Clean numeric, temperature and string fields
"""

import logging
import pandas as pd
from typing import Optional

# repo-local cleaning utilities and logger
from transform.utils_cleaning import (
    normalize_columns,
    clean_numeric_columns,
    clean_string_columns,
    clean_temperature_column,
    clean_date_column,
)
from config.logging_conf import get_logger

logger = get_logger(__name__)


class DailyRecordTransformer:
    # standardized/expected column names after normalization
    EXPECTED_DATE_COL = "timestamp"

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw daily record sheet into cleaned DataFrame.

        Steps:
         - return empty DataFrame if input is None/empty
         - normalize header names to snake_case
         - parse/clean date column if present
         - clean numeric columns (coerce to numeric)
         - clean temperature column if present
         - clean textual columns
        """
        if df is None or df.empty:
            logger.warning("[DailyRecordTransformer] Empty dataset received.")
            return pd.DataFrame()

        # Normalize headers so configured field names match
        df = normalize_columns(df)

        # Parse and standardize date column (handles Excel serials)
        if self.EXPECTED_DATE_COL in df.columns:
            df = clean_date_column(df, self.EXPECTED_DATE_COL)

        # Clean numeric columns: remove separators and coerce to numeric dtype
        numeric_cols = ["feed_eaten_(gram)", "mortality"]
        df = clean_numeric_columns(df, numeric_cols)

        # Clean temperature column if present (supports "temperature")
        if "temperature" in df.columns:
            df = clean_temperature_column(df, "temperature")
        elif "water_temperature" in df.columns:
            df = clean_temperature_column(df, "water_temperature")

        # Clean text fields: trim, lowercase, normalize null tokens
        text_cols = ["fish_behaviour", "feed_size", "note"]
        df = clean_string_columns(df, text_cols)

        logger.info("[DailyRecordTransformer] Transformation complete.")
        return df

