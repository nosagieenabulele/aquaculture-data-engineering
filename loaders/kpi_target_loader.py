"""
SQLAlchemy-based Loader for the KPI Target dataset.

Expects a DataFrame with normalized (snake_case, lowercase) columns:
- record_date, target_biomass, target_avg_weight, fcr_target, mortality_target

Performs a transaction-safe bulk insert and returns the number of inserted rows.
"""

import logging
from typing import Optional, List
import pandas as pd
from sqlalchemy import text
from loaders.base_sql_loader import SQLLoaderBase

logger = logging.getLogger(__name__)


class KPITargetLoader(SQLLoaderBase):
    """
    Loader for the kpi_target table using SQLAlchemy Engine.
    """

    # INSERT uses snake_case column names matching EXPECTED_COLUMNS
    INSERT_QUERY = text("""
        INSERT INTO kpi_target (
            record_date,
            target_biomass,
            target_avg_weight,
            fcr_target,
            mortality_target
        )
        VALUES (
            :record_date,
            :target_biomass,
            :target_avg_weight,
            :fcr_target,
            :mortality_target
        )
    """)

    EXPECTED_COLUMNS: List[str] = [
        "record_date",
        "target_biomass",
        "target_avg_weight",
        "fcr_target",
        "mortality_target"
    ]

    def load(self, df: pd.DataFrame) -> Optional[int]:
        """
        Load the cleaned KPI Target DataFrame into MySQL using SQLAlchemy.

        Args:
            df: Cleaned pandas DataFrame.
        Returns:
            Number of inserted rows or None if empty/failure.
        """

        if df is None or df.empty:
            logger.warning("KPITargetLoader received empty DataFrame.")
            return None

        # Schema validation against expected normalized columns
        missing_cols = [col for col in self.EXPECTED_COLUMNS if col not in df.columns]
        if missing_cols:
            logger.error(f"KPITargetLoader aborted. Missing columns: {missing_cols}")
            return None

        # Convert DataFrame to list of dictionaries for executemany
        payload = df[self.EXPECTED_COLUMNS].to_dict(orient="records")

        if not payload:
            logger.warning("KPITargetLoader received a DataFrame with no rows.")
            return None

        try:
            # Transaction-safe block using engine from SQLLoaderBase
            with self.engine.begin() as conn:
                conn.execute(self.INSERT_QUERY, payload)

            logger.info(f"KPITargetLoader inserted {len(payload)} records successfully.")
            return len(payload)

        except Exception as e:
            logger.error(f"KPITargetLoader failed: {e}", exc_info=True)
            return None
