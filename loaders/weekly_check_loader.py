"""
Loader for Weekly Check dataset using SQLAlchemy.
- Expects transformed DataFrame with snake_case column names (normalize_columns applied).
- Performs a bulk insert inside a transaction and returns number of inserted rows.
"""

import logging
from typing import Optional, List
import pandas as pd
from sqlalchemy import text

from loaders.base_sql_loader import SQLLoaderBase

logger = logging.getLogger(__name__)


class WeeklyCheckLoader(SQLLoaderBase):
    """
    SQLAlchemy-based loader for the weekly_check table.
    """

    # SQL uses snake_case column names and parameter placeholders matching dataframe keys
    INSERT_QUERY = text("""
        INSERT INTO weekly_check (
            record_date,
            pond_id,
            average_weight,
            sgr,
            fcr,
            biomas,
            week_no,
            notes
        )
        VALUES (
            :record_date,
            :pond_id,
            :average_weight,
            :sgr,
            :fcr,
            :biomas,
            :week_no,
            :notes
        )
    """)

    # expected columns after normalize_columns (all lowercase / snake_case)
    EXPECTED_COLUMNS: List[str] = [
        "record_date",
        "pond_id",
        "average_weight",
        "sgr",
        "fcr",
        "biomas",
        "week_no",
        "notes"
    ]

    def load(self, df: pd.DataFrame) -> Optional[int]:
        """
        Insert cleaned Weekly Check records using SQLAlchemy.

        Args:
            df: Cleaned pandas DataFrame (normalized column names)
        Returns:
            Count of inserted rows or None on failure.
        """

        if df is None or df.empty:
            logger.warning("WeeklyCheckLoader received empty DataFrame.")
            return None

        # Validate schema against expected normalized column set
        missing = [col for col in self.EXPECTED_COLUMNS if col not in df.columns]
        if missing:
            logger.error(f"WeeklyCheckLoader missing columns: {missing}")
            return None

        # Convert dataframe → list of dictionaries (SQLAlchemy bulk insert format)
        payload = df[self.EXPECTED_COLUMNS].to_dict(orient="records")

        if not payload:
            logger.warning("WeeklyCheckLoader has no valid rows to insert.")
            return None

        try:
            # Atomic transaction block
            with self.engine.begin() as conn:
                conn.execute(self.INSERT_QUERY, payload)

            logger.info(f"WeeklyCheckLoader inserted {len(payload)} rows successfully.")
            return len(payload)

        except Exception as exc:
            logger.error(f"WeeklyCheckLoader failed: {exc}", exc_info=True)
            return None
