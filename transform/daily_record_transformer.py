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
    EXPECTED_DATE_COL = "record_date"

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
        df = normalize_columns(df, scan_limit=5)
        column_mapping = {
            'timestamp': 'record_date',
            'feed_eaten(gram)': 'feed_eaten',
            'mortality': 'mortality',  
            'fish_behaviour': 'fish_behaviour',
            '': 'feed_size',
            '0': 'feed_eaten',
            'note': 'notes',
            'temperature': 'temperature', 
            'week': 'week_no',
            'pond_id': 'pond_name',
        }
        df =df.rename(columns=column_mapping)
        # Parse and standardize date column (handles Excel serials)
        if self.EXPECTED_DATE_COL in df.columns:
            df = clean_date_column(df, self.EXPECTED_DATE_COL)

        # Clean numeric columns: remove separators and coerce to numeric dtype
        numeric_cols = ["feed_eaten", "mortality", "week_no", "feed_allocated_(grams)"]
        df = clean_numeric_columns(df, numeric_cols)

        # Clean temperature column if present (supports "temperature")
        if "temperature" in df.columns:
            df = clean_temperature_column(df, "temperature")

        df = clean_date_column(df, "record_date")
        
        # Clean text fields: trim, lowercase, normalize null tokens
        text_cols = ["fish_behaviour", "feed_size", "notes"]
        df = clean_string_columns(df, text_cols)

        logger.info("[DailyRecordTransformer] Transformation complete.")

        # Drop rows with more than 70% missing values ---
        total_cols = len(df.columns)
        min_non_nulls = int(total_cols * 0.60) 
        
        # Capture indices of rows to be dropped for logging
        initial_count = len(df)
        df = df.dropna(thresh=min_non_nulls)
        dropped_count = initial_count - len(df)
        
        if dropped_count > 0:
            logger.info(f"[DailyRecordTransformer] Dropped {dropped_count} rows with > 50% missing data.")
        # -------------------------------------------------------
        return df