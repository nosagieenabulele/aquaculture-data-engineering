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

    def transform(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        if df is None or df.empty:
            logger.warning("[InventoryTransformer] Empty raw dataframe")
            return pd.DataFrame()

        # 1. Normalize column names
        df = normalize_columns(df)
        column_mapping = {
            'name': 'name',
            'date_purchased': 'date_purchased',
            'item_no.': 'category',
            'stock_quantity': 'quantity',
            'unit': 'unit',
            'cost_per_item': 'cost',
            'manufacturer': 'manufacturer',
            'date_purchased': 'date_purchased',
        }
        df = df.rename(columns=column_mapping)
        logger.info(f"[InventoryTransformer] Columns after normalization: {list(df.columns)}")
        print (f"Columns after normalization: {list(df.columns)}")

        # 2. Clean string fields
        df = clean_string_columns(df, self.STRING_COLS)

        # 3. Clean numeric fields
        df = clean_numeric_columns(df, self.NUMERIC_COLS)

        # 4. Clean date fields
        for col in self.DATE_COLS:
            df = clean_date_column(df, col)
        # Drop rows with more than 40% missing values ---
        total_cols = len(df.columns)
        min_non_nulls = int(total_cols * 0.60) 
        
        # Capture indices of rows to be dropped for logging
        initial_count = len(df)
        df = df.dropna(thresh=min_non_nulls)
        dropped_count = initial_count - len(df)
        
        if dropped_count > 0:
            logger.info(f"[InventoryTransformer] Dropped {dropped_count} rows with > 40% missing data.")
        # -------------------------------------------------------
        logger.info("[InventoryTransformer] Transformation complete.")
        return df
