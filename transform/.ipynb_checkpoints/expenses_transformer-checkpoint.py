# etl/transform/expenses_transformer.py
"""
Transformer for the Expenses worksheet.
Applies:
- column normalization
- date cleaning
- numeric cleaning
- string corrections
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


class ExpensesTransformer:

    EXPECTED_DATE_COL = "record_date"
    STRING_COLS = ["category", "item", "vendor"]
    NUMERIC_COLS = ["amount", "quantity", "cost"]

    def transform(self, raw_df: pd.DataFrame) -> Optional[Dict[str, pd.DataFrame]]:
        if raw_df is None or raw_df.empty:
            print("[ExpensesTransformer] WARNING: Empty raw dataframe")
            return None

        df = normalize_columns(df)

        # Clean date
        if self.record_date in df.columns:
            df = clean_date_column(df, self.record_date)

        # Clean strings
        df = clean_string_columns(df, self.STRING_COLS)

        # Clean numerics
        df = clean_numeric_columns(df, self.NUMERIC_COLS)

        return df
