# etl/transform/expenses_transformer.py
"""
Transformer for the Expenses worksheet.
Applies:
- column normalization
- date cleaning (Excel serials supported)
- numeric cleaning
- string normalization
"""

import pandas as pd
from typing import Optional

# repo-local cleaning utilities and logger
from transform.utils_cleaning import (
    normalize_columns,
    clean_numeric_columns,
    clean_string_columns,
    clean_date_column,
)
from config.logging_conf import get_logger

logger = get_logger(__name__)


class ExpensesTransformer:
    # expected/standardized column names (normalize_columns -> snake_case)
    EXPECTED_DATE_COL = "record_date"
    STRING_COLS = ["category", "item", "vendor", "description"]
    NUMERIC_COLS = ["cost", "quantity"]

    def transform(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Transform raw expenses sheet into a cleaned DataFrame.

        Steps:
         - return empty DataFrame when input is None/empty
         - normalize column names to snake_case
         - parse/clean date column if present
         - normalize string columns (trim/lower/null)
         - clean numeric columns (remove separators, coerce)
        """
        if df is None or df.empty:
            logger.warning("[ExpensesTransformer] Empty raw dataframe")
            return pd.DataFrame()

        # Normalize headers first so configured column names match
        df = normalize_columns(df, scan_limit=5)

        column_mapping = {
            'timestamp': 'purchase_date',
            'item': 'item',
            'supplier': 'vendor',
            'description': 'description',
            'quantity': 'quantity',
            'cost': 'cost',
        }
        df = df.rename(columns=column_mapping)

        # Clean date column if present
        if self.EXPECTED_DATE_COL in df.columns:
            df = clean_date_column(df, self.EXPECTED_DATE_COL)

        # Clean textual fields
        df = clean_string_columns(df, self.STRING_COLS)

        # Clean numeric fields
        df = clean_numeric_columns(df, self.NUMERIC_COLS)

        logger.info("[ExpensesTransformer] Transformation complete.")
        return df
