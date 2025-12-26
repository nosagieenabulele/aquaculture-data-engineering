"""
Loader for Daily Record cleaned dataset (SQLAlchemy version).
"""
import numpy as np
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

    EXPECTED_COLUMNS: List[str] = [
        "record_date",
        "pond_name",
        "feed_eaten",
        "mortality",
        "fish_behaviour",
        "feed_size",
        "notes",
        "temperature",
        "week_no",

    ]

    INSERT_QUERY = """
        INSERT INTO daily_records (
            record_date, pond_name, feed_eaten, mortality, fish_behaviour,
            feed_size, notes, temperature, week_no
        )
        VALUES (
            :record_date, :pond_name, :feed_eaten, :mortality, :fish_behaviour,
            :feed_size, :notes, :temperature, :week_no
        )
    """

    def load(self, df: pd.DataFrame) -> Optional[int]:
        if df is None or df.empty:
            logger.warning("DailyRecordLoader: Nothing to load.")
            return None

        # 1. Validation: Check if any required columns are missing from the transformation
        missing = [col for col in self.EXPECTED_COLUMNS if col not in df.columns]
        if missing:
            logger.error(f"Schema Mismatch! DataFrame is missing: {missing}")
            return None

        try:
            # 2. Alignment: Filter to only expected columns AND fix the order
            # This ensures df.columns matches the :params in the query
            df_sync = df[self.EXPECTED_COLUMNS].copy()

            # 3. NULL handling: Convert NaN/NAT to None so SQL recognizes them as NULL
            # This follows the ELT principle of handling missing values in-DB
            df_sync = df_sync.replace({np.nan: None})
            
            # 4. Optional: Force specific types if needed (e.g., ensuring numeric columns are floats)
            # df_sync['water_temp'] = pd.to_numeric(df_sync['water_temp'], errors='coerce')

            # 5. Execute bulk insert
            inserted = self._bulk_insert(
                df=df_sync,
                insert_query=self.INSERT_QUERY,
                column_order=self.EXPECTED_COLUMNS,
            )

            logger.info(f"Successfully loaded {inserted} rows into daily_record.")
            return inserted

        except Exception as exc:
            logger.error(f"Database Load Error: {exc}", exc_info=True)
            return None