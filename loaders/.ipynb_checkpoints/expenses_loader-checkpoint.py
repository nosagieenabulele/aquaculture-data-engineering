"""
Loader for Expenses dataset (SQLAlchemy version).
"""

import logging
from typing import Optional, List
import pandas as pd
from etl.loaders.base_sql_loader import SQLLoaderBase

logger = logging.getLogger(__name__)


class ExpensesLoader(SQLLoaderBase):
    """
    Loader for the expenses table using SQLAlchemy.
    """

    INSERT_QUERY = """
        INSERT INTO expenses (
            record_date,
            expense_type,
            vendor,
            amount,
            notes
        )
        VALUES (
            :record_date,
            :category,
            :vendor,
            :amount,
            :notes
        )
    """

    EXPECTED_COLUMNS: List[str] = [
        "record_date",
        "category",
        "vendor",
        "amount",
        "notes"
    ]

    def load(self, df: pd.DataFrame) -> Optional[int]:
        """
        Load the cleaned Expenses DataFrame into MySQL using SQLAlchemy.
        """

        if df is None or df.empty:
            logger.warning("ExpensesLoader received empty DataFrame.")
            return None

        # Validate required columns
        missing_cols = [col for col in self.EXPECTED_COLUMNS if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing columns for ExpensesLoader: {missing_cols}")
            return None

        df_to_insert = df[self.EXPECTED_COLUMNS].copy()

        try:
            inserted = self._bulk_insert(
                df=df_to_insert,
                insert_query=self.INSERT_QUERY,
                column_order=self.EXPECTED_COLUMNS
            )

            logger.info(f"ExpensesLoader inserted {inserted} records successfully.")
            return inserted
        
        except Exception as e:
            logger.error(f"ExpensesLoader failed: {e}")
            return None
