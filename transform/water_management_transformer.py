# etl/transform/water_quality_transformer.py
"""
Transforms Water Quality data into standardized, numeric and datetime formats.
"""

import pandas as pd
from transform.utils_cleaning import (
    normalize_columns,
    clean_numeric_columns,
    clean_string_columns,
    clean_date_column,
    clean_temperature_column,
)
from config.logging_conf import get_logger

logger = get_logger(__name__)

class WaterManagementTransformer:

    DATE_COLS = ["record_date"]
    NUMERIC_COLS = [
        "ph",
        "dissolved_oxygen",
        "ammonia",
        "nitrate",
        "nitrite",
        "alkalinity",
        "total_hardness",
        "carbonate",
    ]
    TEMPERATURE_COL = "water_temperature"
    STRING_COLS = ["water_change"]

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("[WaterManagementTransformer] Transforming water quality...")

        if df is None or df.empty:
            logger.warning("[WaterManagementTransformer] Empty dataset.")
            return pd.DataFrame()

        # normalize headers to snake_case
        df = normalize_columns(df)

        # parse any date columns present
        for col in self.DATE_COLS:
            if col in df.columns:
                df = clean_date_column(df, col)

        # numeric and string cleaning
        df = clean_numeric_columns(df, self.NUMERIC_COLS)
        df = clean_string_columns(df, self.STRING_COLS)

        # temperature cleanup (if present)
        if self.TEMPERATURE_COL in df.columns:
            df = clean_temperature_column(df, self.TEMPERATURE_COL)
        # Drop rows with more than 40% missing values ---
        total_cols = len(df.columns)
        min_non_nulls = int(total_cols * 0.60) 
        
        # Capture indices of rows to be dropped for logging
        initial_count = len(df)
        df = df.dropna(thresh=min_non_nulls)
        dropped_count = initial_count - len(df)
        
        if dropped_count > 0:
            logger.info(f"[WaterManagementTransformer] Dropped {dropped_count} rows with > 40% missing data.")
        # -------------------------------------------------------
        logger.info("[WaterManagementTransformer] Done.")
        return df
