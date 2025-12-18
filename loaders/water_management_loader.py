"""
SQLAlchemy Loader for Water Management dataset.
Handles insertion of transformed water quality records.

Expects DataFrame with normalized (snake_case, lowercase) column names
as produced by transform.utils_cleaning.normalize_columns.
"""

import logging
from typing import Optional, List
import pandas as pd
from sqlalchemy import text

# fix import: use repo-local loaders package (SQLLoaderBase provides self.engine)
from loaders.base_sql_loader import SQLLoaderBase

logger = logging.getLogger(__name__)


class WaterManagementLoader(SQLLoaderBase):
    """
    Loader for the water_management table using SQLAlchemy Engine.
    """

    INSERT_QUERY = text("""
        INSERT INTO water_management (
            record_date,
            pond_id,
            ph,
            dissolved_oxygen,
            water_temperature,
            ammonia,
            nitrite,
            nitrate,
            turbidity,
            conductivity,
            water_depth,
            notes
        )
        VALUES (
            :record_date,
            :pond_id,
            :ph,
            :dissolved_oxygen,
            :water_temperature,
            :ammonia,
            :nitrite,
            :nitrate,
            :turbidity,
            :conductivity,
            :water_depth,
            :notes
        )
    """)

    EXPECTED_COLUMNS: List[str] = [
        "record_date",
        "pond_id",
        "ph",
        "dissolved_oxygen",
        "water_temperature",
        "ammonia",
        "nitrite",
        "nitrate",
        "turbidity",
        "conductivity",
        "water_depth",
        "notes"
    ]

    def load(self, df: pd.DataFrame) -> Optional[int]:
        """
        Insert cleaned water quality dataset into MySQL using SQLAlchemy.

        Args:
            df: DataFrame containing transformed water-quality records.

        Returns:
            Number of inserted rows or None if empty/failure.
        """

        if df is None or df.empty:
            logger.warning("WaterManagementLoader received empty DataFrame.")
            return None

        # Verify input columns (expects normalized/snake_case headers)
        missing_cols = [col for col in self.EXPECTED_COLUMNS if col not in df.columns]
        if missing_cols:
            logger.error(f"WaterManagementLoader aborted. Missing columns: {missing_cols}")
            return None

        # Convert DataFrame → list of dicts for bulk param execution
        payload = df[self.EXPECTED_COLUMNS].to_dict(orient="records")

        if not payload:
            logger.warning("WaterManagementLoader received no valid rows to insert.")
            return None

        try:
            # Safe transaction block: uses engine from SQLLoaderBase
            with self.engine.begin() as conn:
                conn.execute(self.INSERT_QUERY, payload)

            logger.info(f"WaterManagementLoader inserted {len(payload)} records successfully.")
            return len(payload)

        except Exception as exc:
            logger.error(f"WaterManagementLoader failed: {exc}", exc_info=True)
            return None
