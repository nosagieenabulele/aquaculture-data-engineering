# etl/transform/kpi_target_transformer.py
"""
Transformer for KPI Target worksheet.
Cleans numeric fields and standardizes categories.
"""

import pandas as pd
from typing import Optional, Dict

import pandas as pd
from .utils_cleaning import (
    normalize_columns,
    clean_numeric_columns,
    clean_string_columns,
    clean_temperature_column,
    clean_date_column,
)


class KPITargetTransformer:

    NUMERIC_COLS = ["target_biomass", "target_weekly_gain", "target_avg_weight"]

    def transform(self, raw_df: pd.DataFrame) -> Optional[Dict[str, pd.DataFrame]]:
        if raw_df is None or raw_df.empty:
            print("[KPITargetTransformer] WARNING: Empty raw dataframe")
            return None

        df = normalize_columns(df)

        df = clean_string_columns(df, self.STRING_COLS)
        df = clean_numeric_columns(df, self.NUMERIC_COLS)

        return df
        logger = get_logger(__name__)
        logger.info("message")