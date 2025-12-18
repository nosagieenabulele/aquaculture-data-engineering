# etl/transform/inventory_transformer.py
"""
Transformer for the Inventory worksheet.
Used for feed inventory, equipment stock, and consumables.
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


class InventoryTransformer:

    DATE_COLS = ["date_purchased"]
    STRING_COLS = ["item", "category", "manufacturer"]
    NUMERIC_COLS = ["quantity", "cost"]

    def transform(self, raw_df: pd.DataFrame) -> Optional[Dict[str, pd.DataFrame]]:
        if raw_df is None or raw_df.empty:
            print("[InventoryTransformer] WARNING: Empty raw dataframe")
            return None

        df = normalize_columns(df)

        # Clean date columns
        for col in self.date_purchased:
            if col in df.columns:
                df = clean_date_column(df, col)

        # Clean strings
        df = clean_string_columns(df, self.STRING_COLS)

        # Clean numerics
        df = clean_numeric_columns(df, self.NUMERIC_COLS)

        return df
