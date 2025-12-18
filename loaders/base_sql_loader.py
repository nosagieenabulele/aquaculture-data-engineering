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
            insert_query: SQL insert statement (string) or SQLAlchemy TextClause
            column_order: Optional list specifying column order to match SQL insert

        Returns:
            Number of inserted rows (0 on failure or empty input)
        """
        if df is None or df.empty:
            logger.warning("Bulk insert skipped: DataFrame is empty.")
            return 0

        # Ensure columns are in the requested order (if provided)
        if column_order:
            df_to_insert = df[column_order]
        else:
            df_to_insert = df

        # Convert to list-of-dicts for SQLAlchemy execution
        records = df_to_insert.to_dict(orient="records")

        # Accept either a raw SQL string or a SQLAlchemy TextClause
        query_obj = insert_query if not isinstance(insert_query, str) else text(insert_query)

        try:
            # Use a transaction scoped to the engine; executemany style with parameter lists
            with self.engine.begin() as conn:
                conn.execute(query_obj, records)

            inserted = len(records)
            logger.info(f"Inserted {inserted} records.")
            return inserted

        except Exception as exc:
            # Log full stack trace for easier debugging
            logger.exception("Bulk insert failed: %s", exc)
            return 0
