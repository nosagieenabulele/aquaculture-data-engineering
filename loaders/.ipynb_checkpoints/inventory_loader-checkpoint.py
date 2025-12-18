"""
SQLAlchemy-based Loader for the Inventory dataset.
"""

import logging
from typing import Optional, List
import pandas as pd
from sqlalchemy import text

from etl.loaders.base_sql_loader import SQLLoaderBase

logger = logging.getLogger(__name__)


class InventoryLoader(SQLLoaderBase):
    """
    Loader for the inventory table using SQLAlchemy Engine.

    This version:
    - Uses SQLAlchemy's transaction management.
    - Uses named parameter binding for safety.
    - Inserts via bulk execution (efficient for ETL).
    """

    # SQLAlchemy uses named params (:param_name)
    INSERT_QUERY = text("""
        INSERT INTO inventory (
            item_name, category, quantity, unit, unit_price,
            supplier, last_updated
        )
        VALUES (
            :item, :category, :quantity, :unit, :cost,
            :manufacturer, :date_purchased
        )
    """)

    EXPECTED_COLUMNS: List[str] = [
        "item",
        "category",
        "quantity",
        "unit",
        "cost",
        "manufacturer",
        "date_purchased"
    ]

    def load(self, df: pd.DataFrame) -> Optional[int]:
        """
        Load the cleaned Inventory DataFrame into MySQL using SQLAlchemy.

        Args:
            df: Cleaned DataFrame
        Returns:
            Number of inserted rows or None if empty/failure.
        """

        if df is None or df.empty:
            logger.warning("InventoryLoader received empty DataFrame.")
            return None

        # Validate schema
        missing_cols = [c for c in self.EXPECTED_COLUMNS if c not in df.columns]
        if missing_cols:
            logger.error(f"InventoryLoader aborted. Missing columns: {missing_cols}")
            return None

        # SQLAlchemy requires dict-per-row
        payload = df[self.EXPECTED_COLUMNS].to_dict(orient="records")

        if len(payload) == 0:
            logger.warning("InventoryLoader received a DataFrame with no rows.")
            return None

        try:
            with self.engine.begin() as conn:
                conn.execute(self.INSERT_QUERY, payload)

            logger.info(f"InventoryLoader inserted {len(payload)} records successfully.")
            return len(payload)

        except Exception as e:
            logger.error(f"InventoryLoader failed: {e}", exc_info=True)
            return None
