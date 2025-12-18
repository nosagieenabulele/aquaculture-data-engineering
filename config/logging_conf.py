"""
Production-grade logging configuration for the ETL system.

This module defines:
- Rotating log files (prevents unlimited log growth)
- Console logging for development
- Standardized log formats
- Centralized logger creation used by all ETL modules
"""
import logging
import logging.handlers
from pathlib import Path
# use repo-local settings module (remove 'etl.' prefix)
from config.settings import LOGGING

# -------------------------------------------------------------------
# 1. Create base log directory if missing (guarded call)
# -------------------------------------------------------------------
# Some settings implementations expose an ensure_directories helper;
# call it if present to create log folders before opening files.
if hasattr(LOGGING, "ensure_directories"):
    try:
        LOGGING.ensure_directories()
    except Exception:
        # avoid raising during import; log the issue and continue
        logging.getLogger(__name__).exception("LOGGING.ensure_directories() failed")

# ensure LOG_DIR is a Path object
LOG_DIR = Path(getattr(LOGGING, "log_dir", Path.cwd()))


# -------------------------------------------------------------------
# 2. Formatters (standard enterprise formatting)
# -------------------------------------------------------------------
FORMAT = (
    "%(asctime)s | %(levelname)s | %(name)s | "
    "%(funcName)s | line %(lineno)d | %(message)s"
)

formatter = logging.Formatter(FORMAT)


# -------------------------------------------------------------------
# 3. File Handler (Rotating – production safe)
# -------------------------------------------------------------------
# file_handler configured to write to LOG_DIR/etl.log
file_handler = logging.handlers.RotatingFileHandler(
    filename=LOG_DIR / "etl.log",
    maxBytes=10 * 1024 * 1024,   # 10 MB per file
    backupCount=5,               # keep 5 old logs
    encoding="utf-8"
)
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)


# -------------------------------------------------------------------
# 4. Console Handler (helpful in development)
# -------------------------------------------------------------------
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.DEBUG)


# -------------------------------------------------------------------
# 5. Logger Factory Function
# -------------------------------------------------------------------
def get_logger(name: str) -> logging.Logger:
    """Return a preconfigured logger for any module."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Avoid adding handlers multiple times
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    logger.propagate = False
    return logger


# -------------------------------------------------------------------
# Example root logger initialization (optional)
# -------------------------------------------------------------------
root_logger = get_logger("ETL")
root_logger.info("Logging system initialized.")
