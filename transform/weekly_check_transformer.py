# etl/transform/weekly_check_transformer.py
"""
Transformer for the Weekly Check worksheet.
Includes numeric cleaning and date standardization.
"""

import pandas as pd
from typing import Optional, Dict

# use repo-local utilities and logging
from transform.utils_cleaning import clean_string_columns, clean_numeric_columns, clean_date_column, normalize_columns
from config.logging_conf import get_logger

logger = get_logger(__name__)

class WeeklyCheckTransformer:

    DATE_COL = "record_date"
    STRING_COLS = ["notes"]
    NUMERIC_COLS = ["average_weight", "SGR", "FCR", "biomas", "week_no"]

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            logger.warning("[WeeklyCheckTransformer] Empty raw dataframe")
            return pd.DataFrame()  # return empty df for consistency

        # Normalize columns
        df = normalize_columns(df)

        # Clean date column
        if self.DATE_COL in df.columns:
            df = clean_date_column(df, self.DATE_COL)

        # Clean string columns
        df = clean_string_columns(df, self.STRING_COLS)

        # Clean numeric columns
        df = clean_numeric_columns(df, self.NUMERIC_COLS)

        logger.info("[WeeklyCheckTransformer] Transformation complete.")
        return df