# etl/transform/daily_record_transformer.py
"""
Transforms the raw Daily Record sheet into validated, cleaned data.
"""

import pandas as pd
from .utils_cleaning import (
    normalize_columns,
    clean_numeric_columns,
    clean_string_columns,
    clean_temperature_column,
    clean_date_column,
)


class DailyRecordTransformer:

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        print("[daily_record_transformer] Transforming daily record...")

        if df is None or df.empty:
            print("[daily_record_transformer] Empty dataset received.")
            return df

        df = normalize_columns(df)

        # Clean dates
        df = clean_date_column(df, "record_date")

        # Clean numeric
        numeric_cols = ["feed_eaten", "mortality"]
        df = clean_numeric_columns(df, numeric_cols)

        # Clean temperature
        df = clean_temperature_column(df, "water_temp")

        # Clean text fields
        text_cols = ["fish_behaviour", "feed_size", "notes"]
        df = clean_string_columns(df, text_cols)

        print("[daily_record_transformer] Done.")
        return df
