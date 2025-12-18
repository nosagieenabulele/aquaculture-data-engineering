# transform/inventory_transformer.py

import pandas as pd
from typing import Optional

from transform.utils_cleaning import (
    normalize_columns,
    clean_numeric_columns,
    clean_string_columns,
    clean_date_column,
)
from config.logging_conf import get_logger

logger = get_logger(__name__)


class InventoryTransformer:
    """
    Transformer for Inventory worksheet.

    Responsibilities:
    - Normalize column headers
    - Clean string fields
    - Clean numeric fields
    - Parse date fields
    """

    # Columns expected from inventory sheet
    NUMERIC_COLS = ["quantity", "cost"]
    STRING_COLS = ["name", "category", "manufacturer"]
    DATE_COLS = ["date_purchased"]

    def transform(self, raw_df: pd.DataFrame) -> Optional[pd.DataFrame]:
        if raw_df is None or raw_df.empty:
            logger.warning("[InventoryTransformer] Empty raw dataframe")
            return pd.DataFrame()

        # 1. Normalize column names
        df = normalize_columns(raw_df)

        # 2. Clean string fields
        df = clean_string_columns(df, self.STRING_COLS)

        # 3. Clean numeric fields
        df = clean_numeric_columns(df, self.NUMERIC_COLS)

        # 4. Clean date fields
        for col in self.DATE_COLS:
            df = clean_date_column(df, col)

        logger.info("[InventoryTransformer] Transformation complete.")
        return df
