"""Orchestrator for all ETL pipelines.
Builds the DB engine, resolves pipeline classes dynamically and runs each pipeline.
"""

import logging
import importlib
from sqlalchemy import create_engine
from config.settings import GOOGLE_SHEETS, MYSQL_CONFIG

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ETL_Run")

# Check availability of the pymysql DB-API driver (informative only)
try:
    import pymysql  # optional, required if using mysql+pymysql
except Exception:
    logger.debug("pymysql not available - ensure 'pymysql' is installed if using mysql+pymysql driver.")

# create SQLAlchemy engine (use provided connection_url if present)
if hasattr(MYSQL_CONFIG, "connection_url") and MYSQL_CONFIG.connection_url:
    # use explicit connection_url from settings when available
    engine = create_engine(MYSQL_CONFIG.connection_url)
else:
    # build a pymysql URL fallback (requires pymysql installed in environment)
    try:
        # NOTE: this fallback assumes pymysql is intended as the driver.
        engine = create_engine(
            f"mysql+pymysql://{MYSQL_CONFIG.user}:{MYSQL_CONFIG.password}@"
            f"{MYSQL_CONFIG.host}:{getattr(MYSQL_CONFIG, 'port', 3306)}/{MYSQL_CONFIG.database}"
        )
    except Exception as exc:
        logger.exception("Failed to create SQLAlchemy engine. Ensure SQLAlchemy and pymysql are installed and MYSQL_CONFIG fields are correct.")
        raise

# Google Sheet URLs for each dataset (constructed from settings)
# Guard against a missing spreadsheet_id to avoid building invalid URLs.
if not getattr(GOOGLE_SHEETS, "spreadsheet_id", ""):
    logger.error("GOOGLE_SHEETS.spreadsheet_id is not set. Set SPREADSHEET_ID in your .env before running.")
    SHEET_URLS = {}
else:
    SHEET_URLS = {
        name: f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEETS.spreadsheet_id}/edit#gid={idx}"
        for name, idx in getattr(GOOGLE_SHEETS, "sheet_index_map", {}).items()
    }

def _resolve_pipeline_class(ref):
    # Accepts either "module:ClassName" string or a class object; returns the class.
    if isinstance(ref, str):
        # Expect "module.path:ClassName"
        if ":" not in ref:
            raise ValueError("pipeline reference string must be 'module.path:ClassName'")
        module_name, class_name = ref.split(":", 1)
        mod = importlib.import_module(module_name)
        return getattr(mod, class_name)
    return ref

def run_pipeline(pipeline_ref, sheet_url):
    pipeline_class = _resolve_pipeline_class(pipeline_ref)
    pipeline = pipeline_class(sheet_url, engine)
    logger.info(f"Running {pipeline_class.__name__}...")
    try:
        extracted = pipeline.extract()
        if extracted is None:
            logger.warning(f"{pipeline_class.__name__}: Extraction returned nothing.")
            return


        transformed = pipeline.transform(extracted)
        if transformed is None:
            logger.warning(f"{pipeline_class.__name__}: Transformation returned nothing.")
            return

        # ✅ TRANSFORMATION METRICS
        logger.info(
            f"{pipeline_class.__name__}: Transformed {len(transformed)} rows "
            f"with columns: {list(transformed.columns)}"
        )

# --- ADDED PRINT STATEMENTS ---
        print("\n" + "="*50)
        print(f"TRANSFORMED DATA PREVIEW: {pipeline_class.__name__}")
        print("="*50)
        print(transformed.iloc[900:910])  # Prints rows 900-910
        print("-"*50)
        print(f"Total Rows: {len(transformed)} | Columns: {list(transformed.columns)}")
        print("="*50 + "\n")
        # ------------------------------

        print('fnull_counts:', transformed.isna().sum())
        rows_with_any_nulls = transformed[transformed.isnull().any(axis=1)].index.tolist()
        print(f"Indices of rows with at least one null value: {rows_with_any_nulls}")
        print(f'dataset dtypes:\n{transformed.dtypes}')


        inserted = pipeline.load(transformed)
        logger.info(f"{pipeline_class.__name__}: Inserted {inserted or 0} rows.")
    except Exception as e:
        logger.exception(f"{pipeline_class.__name__} failed: {e}")


if __name__ == "__main__":
    # map pipeline keys to module:Class strings for dynamic import
    # NOTE: keys here must match keys in GOOGLE_SHEETS.sheet_index_map (zero-based indices)
    PIPELINE_MAP = {
        "daily_record": "pipelines.daily_record_pipeline:DailyRecordPipeline",
       # "expenses": "pipelines.expenses_pipeline:ExpensesPipeline",
       # "inventory": "pipelines.inventory_pipeline:InventoryPipeline",
        #"kpi_target": "pipelines.kpi_target_pipeline:KPITargetPipeline",
      #  "weekly_check": "pipelines.weekly_check_pipeline:WeeklyCheckPipeline",
    }

    for name, ref in PIPELINE_MAP.items():
        sheet_url = SHEET_URLS.get(name)
        if not sheet_url:
            logger.error("Missing sheet URL for %s (check GOOGLE_SHEETS.sheet_index_map)", name)
            continue
        run_pipeline(ref, sheet_url)
