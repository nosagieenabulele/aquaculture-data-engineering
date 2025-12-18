import logging
from etl.config.settings import MYSQL_CONFIG
from etl.pipelines.daily_record_pipeline import DailyRecordPipeline
from etl.pipelines.expenses_pipeline import ExpensesPipeline
from etl.pipelines.inventory_pipeline import InventoryPipeline
from etl.pipelines.kpi_target_pipeline import KPITargetPipeline
from etl.pipelines.weekly_check_pipeline import WeeklyCheckPipeline

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ETL_Run")

# Database connection config
db_config = {
    "host": MYSQL_CONFIG.host,
    "user": MYSQL_CONFIG.user,
    "password": MYSQL_CONFIG.password,
    "database": MYSQL_CONFIG.database,
}

# Google Sheet URLs for each dataset
SHEET_URLS = {
    "daily_record": "https://docs.google.com/spreadsheets/d/your_daily_record_id",
    "expenses": "https://docs.google.com/spreadsheets/d/your_expenses_id",
    "inventory": "https://docs.google.com/spreadsheets/d/your_inventory_id",
    "kpi_target": "https://docs.google.com/spreadsheets/d/your_kpi_target_id",
    "weekly_check": "https://docs.google.com/spreadsheets/d/your_weekly_check_id",
}

def run_pipeline(pipeline_class, sheet_url):
    pipeline = pipeline_class(sheet_url, db_config)
    logger.info(f"Running {pipeline_class.__name__}...")
    
    extracted = pipeline.extract()
    if extracted is None:
        logger.warning(f"{pipeline_class.__name__}: Extraction returned nothing.")
        return

    transformed = pipeline.transform(extracted)
    if transformed is None:
        logger.warning(f"{pipeline_class.__name__}: Transformation returned nothing.")
        return

    inserted = pipeline.load(transformed)
    logger.info(f"{pipeline_class.__name__}: Inserted {inserted} rows.")


if __name__ == "__main__":
    run_pipeline(DailyRecordPipeline, SHEET_URLS["daily_record"])
    run_pipeline(ExpensesPipeline, SHEET_URLS["expenses"])
    run_pipeline(InventoryPipeline, SHEET_URLS["inventory"])
    run_pipeline(KPITargetPipeline, SHEET_URLS["kpi_target"])
    run_pipeline(WeeklyCheckPipeline, SHEET_URLS["weekly_check"])
