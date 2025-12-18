# etl/transform/water_quality_transformer.py
"""
Transforms Water Quality data into standardized, numeric and datetime formats.
"""

import pandas as pd
from .utils_cleaning import (
    normalize_columns,
    clean_numeric_columns,
    clean_string_columns,
    clean_date_column,
    clean_temperature_column,
)


class WaterManagementTransformer:

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        print("[water_quality_transformer] Transforming water quality...")

        if df is None or df.empty:
            print("[water_quality_transformer] Empty dataset.")
            return df

        df = normalize_columns(df)

        for col in self.record_date:
            if col in df.columns:
                df = clean_date_column(df, col)

        numeric_cols = ["ph", "dissolved_oxygen", "ammonia", "nitrate", "nitrite", "alkalinity", "total_hardness", "carbonate"]
        df = clean_numeric_columns(df, numeric_cols)

        df = clean_string_columns(df, ["water_change"])

                # Clean temperature
        df = clean_temperature_column(df, "water_temperature")

        logger = get_logger(__name__)
        logger.info("message")

        print("[water_quality_transformer] Done.")
        return df
