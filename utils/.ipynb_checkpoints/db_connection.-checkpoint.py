# utils/db_connection.py
"""
MySQL connection helper.

Provides SQLAlchemy engine creation for loaders and pipelines.
Uses version-1 logging (simple print statements).
"""

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from config.settings import MYSQL_CONFIG


def get_mysql_engine():
    """
    Create and return a SQLAlchemy engine using project settings.

    Returns
    -------
    sqlalchemy.Engine or None
    """
    try:
        engine = create_engine(
            MYSQL_CONFIG.connection_url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )
        print("[db_connection] MySQL engine created.")
        return engine
    except SQLAlchemyError as exc:
        print(f"[db_connection] ERROR - Failed to create MySQL engine: {exc}")
        return None
