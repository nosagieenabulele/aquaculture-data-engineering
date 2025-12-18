"""
Centralized configuration settings for the ETL system.
Loads environment variables, defines project-wide constants,
and exposes typed configuration objects for each subsystem.

Used by extractors, transformers, loaders, and orchestration pipelines
to ensure consistent and predictable runtime behavior.
"""

import os
from pathlib import Path
from dataclasses import dataclass
from dotenv import load_dotenv

# -------------------------------------------------------------------
# Load environment variables (.env file)
# -------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")     # <-- Use standard name

# -------------------------------------------------------------------
# Database Configuration
# -------------------------------------------------------------------

@dataclass(frozen=True)
class MySQLConfig:
    host: str = os.getenv("DB_HOST", "localhost")
    port: int = int(os.getenv("DB_PORT", 3306))
    user: str = os.getenv("DB_USER", "root")
    password: str = os.getenv("DB_PASSWORD", "")  # never store default password
    database: str = os.getenv("DB_NAME", "fishfarmsdb")

    @property
    def connection_url(self) -> str:
        """Formatted SQLAlchemy MySQL connection URI."""
        return (
            f"mysql+pymysql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


MYSQL_CONFIG = MySQLConfig()

# -------------------------------------------------------------------
# Google Sheets Configuration
# -------------------------------------------------------------------

@dataclass(frozen=True)
class GoogleSheetsConfig:
    service_account_file: Path = Path(
        os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "")
    )
    spreadsheet_id: str = os.getenv("SPREADSHEET_ID", "")

    # Single source of truth for worksheet indices
    sheet_index_map = {
        "daily_record": 6,
        "expenses": 4,
        "inventory": 2,
        "weekly_check": 12,
        "water_management": 7,
        "kpi_target": 14,
    }


GOOGLE_SHEETS = GoogleSheetsConfig()

# -------------------------------------------------------------------
# Logging Configuration
# -------------------------------------------------------------------

@dataclass(frozen=True)
class LoggingConfig:
    log_dir: Path = BASE_DIR / "logs"
    etl_log_file: Path = BASE_DIR / "logs" / "etl.log"

    def ensure_directories(self) -> None:
        if not self.log_dir.exists():
            self.log_dir.mkdir(parents=True, exist_ok=True)


LOGGING = LoggingConfig()
LOGGING.ensure_directories()

# -------------------------------------------------------------------
# Data Directory Paths
# -------------------------------------------------------------------

@dataclass(frozen=True)
class DataPaths:
    raw_dir: Path = BASE_DIR / "data" / "raw"
    processed_dir: Path = BASE_DIR / "data" / "processed"
    temp_dir: Path = BASE_DIR / "data" / "temp"

    def ensure_directories(self) -> None:
        for folder in [self.raw_dir, self.processed_dir, self.temp_dir]:
            folder.mkdir(parents=True, exist_ok=True)


DATA_PATHS = DataPaths()
DATA_PATHS.ensure_directories()

# -------------------------------------------------------------------
# ETL Runtime Configuration
# -------------------------------------------------------------------

@dataclass(frozen=True)
class ETLConfig:
    chunk_size: int = 5000
    retry_attempts: int = 3
    retry_delay_seconds: int = 5
    timezone: str = "Africa/Lagos"
    pipeline_owner: str = "ETL System"


ETL_SETTINGS = ETLConfig()

# -------------------------------------------------------------------
# Export Namespace
# -------------------------------------------------------------------

__all__ = [
    "MYSQL_CONFIG",
    "GOOGLE_SHEETS",
    "LOGGING",
    "DATA_PATHS",
    "ETL_SETTINGS",
]
