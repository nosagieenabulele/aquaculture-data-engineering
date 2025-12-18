# etl/loaders/dry_run_loader.py
"""
Dry-run loader for SQLAlchemy-based pipelines.
Logs intended inserts without touching the database.
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


class DryRunLoader:
    """
    Mimics SQLAlchemy loaders but does not execute any inserts.
    """

    def __init__(self, engine=None):
        """
        Accepts SQLAlchemy engine for API consistency (optional).
        """
        self.engine = engine

    def load(self, df: pd.DataFrame) -> int:
        """
        Simulate loading a DataFrame into a table.

        Args:
            df: pandas DataFrame to "load"

        Returns:
            Number of rows that would have been inserted
        """
        if df is None or df.empty:
            logger.warning("[DryRunLoader] Received empty DataFrame. Nothing to insert.")
            return 0

        logger.info(f"[DryRunLoader] Would insert {len(df)} rows into the target table.")
        logger.debug(f"[DryRunLoader] Columns: {df.columns.tolist()}")
        logger.debug(f"[DryRunLoader] Sample data:\n{df.head()}")

        # Return number of rows as if inserted
        return len(df)
