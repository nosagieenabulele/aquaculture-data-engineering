"""
Loader for Daily Record cleaned dataset (SQLAlchemy version).
"""

import logging
from typing import Optional, List
import pandas as pd
from loaders.base_sql_loader import SQLLoaderBase  # fixed import: use local loaders package

logger = logging.getLogger(__name__)


class DailyRecordLoader(SQLLoaderBase):
    """
    Loader for the daily_record table using SQLAlchemy.

    Expects transformed DataFrame with normalized (snake_case) headers:
      record_date, pond_id, feed_eaten, mortality, fish_behaviour,
      feed_size, notes, water_temp

    Uses SQLLoaderBase._bulk_insert for transaction-safe bulk inserts.
    """

    # Use normalized column name 'record_date' to match DataFrame keys and EXPECTED_COLUMNS
    INSERT_QUERY = """
        INSERT INTO daily_record (
            record_date, pond_id, feed_eaten, mortality, fish_behaviour,
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
        Load the cleaned daily record DataFrame into MySQL.

        - Validates expected columns are present
        - Uses _bulk_insert helper from SQLLoaderBase for performant, parameterized inserts
        """
        if df is None or df.empty:
            logger.warning("DailyRecordLoader received empty DataFrame.")
            return None

        # Validate required columns exist
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
            logger.error(f"DailyRecordLoader failed: {exc}", exc_info=True)
            return None
