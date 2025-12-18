"""
SQLAlchemy-based Loader for the Inventory dataset.

Expects a DataFrame with normalized (snake_case, lowercase) columns produced by
transform.utils_cleaning.normalize_columns.

DB column mapping (DataFrame key -> DB column):
  - item            -> item_name
  - cost            -> unit_price
  - manufacturer    -> supplier
  - date_purchased  -> last_updated
"""

import logging
from typing import Optional, List
import pandas as pd
from sqlalchemy import text

# fixed import: use repo-local loaders package (SQLLoaderBase provides self.engine)
from loaders.base_sql_loader import SQLLoaderBase

logger = logging.getLogger(__name__)


class InventoryLoader(SQLLoaderBase):
    """
    Loader for the inventory table using SQLAlchemy Engine.

    - Uses named parameter binding for safety.
    - Inserts via bulk execution inside a transaction.
    """

    # INSERT maps DataFrame keys (named params) to the target DB columns.
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
            df: Cleaned DataFrame with normalized headers.
        Returns:
            Number of inserted rows or None if empty/failure.
        """
        if df is None or df.empty:
            logger.warning("InventoryLoader received empty DataFrame.")
            return None

        # Validate schema against expected normalized columns
        missing_cols = [c for c in self.EXPECTED_COLUMNS if c not in df.columns]
        if missing_cols:
            logger.error(f"InventoryLoader aborted. Missing columns: {missing_cols}")
            return None

        # Convert DataFrame → list of dicts for bulk execution
        payload = df[self.EXPECTED_COLUMNS].to_dict(orient="records")
        if not payload:
            logger.warning("InventoryLoader received a DataFrame with no rows.")
            return None

        try:
            # Transaction-safe execution using engine from SQLLoaderBase
            with self.engine.begin() as conn:
                conn.execute(self.INSERT_QUERY, payload)

            logger.info(f"InventoryLoader inserted {len(payload)} records successfully.")
            return len(payload)

        except Exception as e:
            logger.error(f"InventoryLoader failed: {e}", exc_info=True)
            return None
