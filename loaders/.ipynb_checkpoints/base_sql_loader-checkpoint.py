"""
Base class for SQL loaders using SQLAlchemy.
Provides reusable insert logic, connection handling, and logging.
"""

import logging
from typing import Optional, List
import pandas as pd
from sqlalchemy import text, Engine

logger = logging.getLogger(__name__)


class SQLLoaderBase:
    """
    Base class for SQL loading operations using SQLAlchemy.
    """

    def __init__(self, engine: Engine):
        """
        Accepts a SQLAlchemy engine that will be reused by concrete loaders.
        """
        if not isinstance(engine, Engine):
            raise ValueError("engine must be a SQLAlchemy Engine instance")

        self.engine = engine

    def _bulk_insert(
        self,
        df: pd.DataFrame,
        insert_query: str,
        column_order: Optional[List[str]] = None,
    ) -> int:
        """
        Execute bulk insert for a DataFrame using SQLAlchemy.

        Args:
            df: pandas DataFrame to insert
            insert_query: SQL insert statement using named parameters (:colname)
            column_order: Optional list specifying column order to match SQL insert

        Returns:
            Number of inserted rows
        """
        if df is None or df.empty:
            logger.warning("Bulk insert skipped: DataFrame is empty.")
            return 0

        # If explicit column order is required
        if column_order:
            df_to_insert = df[column_order]
        else:
            df_to_insert = df

        # Convert to list-of-dicts for SQLAlchemy
        records = df_to_insert.to_dict(orient="records")

        try:
            with self.engine.begin() as conn:
                conn.execute(text(insert_query), records)

            inserted = len(records)
            logger.info(f"Inserted {inserted} records.")
            return inserted

        except Exception as exc:
            logger.error(f"Bulk insert failed: {exc}")
            return 0
