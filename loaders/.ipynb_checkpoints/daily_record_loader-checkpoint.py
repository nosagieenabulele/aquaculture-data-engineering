"""
Loader for Daily Record cleaned dataset (SQLAlchemy version).
"""

import logging
from typing import Optional, List
import pandas as pd
from etl.loaders.base_sql_loader import SQLLoaderBase

logger = logging.getLogger(__name__)


class DailyRecordLoader(SQLLoaderBase):
    """
    Loader for the daily_record table using SQLAlchemy.
    """

    # SQLAlchemy uses named parameters (:param) instead of %s
    INSERT_QUERY = """
        INSERT INTO daily_record (
            date, pond_id, feed_eaten, mortality, fish_behaviour,
            feed_size, notes, water_temp
        )
        VALUES (
            :record_date, :pond_id, :feed_eaten, :mortality, :fish_behaviour,
            :feed_size, :notes, :water_temp
        )
    """

    EXPECTED_COLUMNS: List[str] = [
        "record_date",
        "pond_id",
        "feed_eaten",
        "mortality",
        "fish_behaviour",
        "feed_size",
        "notes",
        "water_temp",
    ]

    def load(self, df: pd.DataFrame) -> Optional[int]:
        """
        Load the cleaned daily record DataFrame into MySQL using SQLAlchemy.

        Args:
            df: Cleaned pandas DataFrame
        Returns:
            Number of inserted rows or None if empty/failure
        """
        if df is None or df.empty:
            logger.warning("DailyRecordLoader received empty DataFrame.")
            return None

        # Ensure required columns exist
        missing = [col for col in self.EXPECTED_COLUMNS if col not in df.columns]
        if missing:
            logger.error(f"Missing required columns in DailyRecordLoader: {missing}")
            return None

        df_to_insert = df[self.EXPECTED_COLUMNS].copy()

        try:
            inserted = self._bulk_insert(
                df=df_to_insert,
                insert_query=self.INSERT_QUERY,
                column_order=self.EXPECTED_COLUMNS,
            )

            logger.info(f"DailyRecordLoader inserted {inserted} records successfully.")
            return inserted

        except Exception as exc:
            logger.error(f"DailyRecordLoader failed: {exc}")
            return None
