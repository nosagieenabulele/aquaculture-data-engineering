"""Pipeline: Expenses
- Extracts expenses sheet, transforms and loads into DB.
- Follows extract -> transform -> load contract from ETL pipeline base.
"""

import logging
# use repo-local modules (remove 'etl.' prefix)
from pipelines.base_pipeline import ETLPipelineBase
from extract.google_sheet_extractor import GoogleSheetExtractor
from transform.expenses_transformer import ExpensesTransformer
from loaders.expenses_loader import ExpensesLoader
from config.settings import GOOGLE_SHEETS
import pandas as pd

logger = logging.getLogger(__name__)


class ExpensesPipeline(ETLPipelineBase):

    def __init__(self, sheet_url: str, engine):
        super().__init__()
        # initialize extractor with service account file and connect to sheet
        self.extractor = GoogleSheetExtractor(service_account_path=str(GOOGLE_SHEETS.service_account_file))
        self.extractor.connect(sheet_url)
        # transformer and loader instances
        self.transformer = ExpensesTransformer()
        self.loader = ExpensesLoader(engine)  # SQLAlchemy engine now

    def extract(self) -> pd.DataFrame | None:
        try:
            # read configured worksheet index for expenses
            return self.extractor.extract_sheet(index=GOOGLE_SHEETS.sheet_index_map["expenses"])
        except Exception as exc:
            logger.error(f"ExpensesPipeline extract failed: {exc}")
            return None

    def transform(self, extracted: pd.DataFrame | None) -> pd.DataFrame | None:
        try:
            return self.transformer.transform(extracted)
        except Exception as exc:
            logger.error(f"ExpensesPipeline transform failed: {exc}")
            return None

    def load(self, transformed: pd.DataFrame | None) -> int | None:
        try:
            return self.loader.load(transformed)
        except Exception as exc:
            logger.error(f"ExpensesPipeline load failed: {exc}")
            return None
