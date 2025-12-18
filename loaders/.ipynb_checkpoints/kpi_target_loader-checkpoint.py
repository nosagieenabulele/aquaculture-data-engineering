"""
SQLAlchemy-based Loader for the KPI Target dataset.
"""

import logging
from typing import Optional, List
import pandas as pd
from sqlalchemy import text

from etl.loaders.base_sql_loader import SQLLoaderBase

logger = logging.getLogger(__name__)


class KPITargetLoader(SQLLoaderBase):
    """
    Loader for the kpi_target table using SQLAlchemy Engine.

    This version:
    - Uses SQLAlchemy text() with named parameters.
    - Supports clean bulk insertion.
    - Uses engine.begin() for safe atomic commits.
    """

    INSERT_QUERY = text("""
        INSERT INTO kpi_target (
            record_date,
            forecast_biomass,
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

        # Schema validation
        missing_cols = [col for col in self.EXPECTED_COLUMNS if col not in df.columns]
        if missing_cols:
            logger.error(f"KPITargetLoader aborted. Missing columns: {missing_cols}")
            return None

        # Convert DataFrame to list of dictionaries
        payload = df[self.EXPECTED_COLUMNS].to_dict(orient="records")

        if len(payload) == 0:
            logger.warning("KPITargetLoader received a DataFrame with no rows.")
            return None

        try:
            # Transaction-safe block
            with self.engine.begin() as conn:
                conn.execute(self.INSTALL_QUERY, payload)

            logger.info(f"KPITargetLoader inserted {len(payload)} records successfully.")
            return len(payload)

        except Exception as e:
            logger.error(f"KPITargetLoader failed: {e}", exc_info=True)
            return None
