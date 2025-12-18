# etl/transform/kpi_target_transformer.py

import pandas as pd
from typing import Optional

from transform.utils_cleaning import (
    normalize_columns,
    clean_numeric_columns,
    clean_string_columns,
    clean_date_column,
    clean_temperature_column,
)
from config.logging_conf import get_logger

logger = get_logger(__name__)

class KPITargetTransformer:
    """
    Transformer for KPI Target worksheet.
    - Normalizes headers
    - Cleans string fields
    - Cleans numeric fields
    """
    NUMERIC_COLS = ["target_biomass", "target_weekly_gain", "target_avg_weight"]
    STRING_COLS = ["category", "notes"]

    def transform(self, raw_df: pd.DataFrame) -> Optional[pd.DataFrame]:
        if raw_df is None or raw_df.empty:
            logger.warning("[KPITargetTransformer] Empty raw dataframe")
            return pd.DataFrame()

        # normalize headers and clean fields
        df = normalize_columns(raw_df)
        df = clean_string_columns(df, self.STRING_COLS)
        df = clean_numeric_columns(df, self.NUMERIC_COLS)

        logger.info("[KPITargetTransformer] Transformation complete.")
        return df