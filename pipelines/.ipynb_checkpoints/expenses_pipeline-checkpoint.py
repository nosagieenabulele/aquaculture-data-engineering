# etl/pipelines/expenses_pipeline.py

import logging
from etl.pipelines.base_pipeline import ETLPipelineBase
from etl.extract.google_sheet_extractor import GoogleSheetExtractor
from etl.transform.expenses_transformer import ExpensesTransformer
from etl.loaders.expenses_loader import ExpensesLoader
from etl.config.settings import GOOGLE_SHEETS
import pandas as pd

logger = logging.getLogger(__name__)


class ExpensesPipeline(ETLPipelineBase):

    def __init__(self, sheet_url: str, engine):
        super().__init__()
        self.extractor = GoogleSheetExtractor(service_account_path=str(GOOGLE_SHEETS.service_account_file))
        self.extractor.connect(sheet_url)
        self.transformer = ExpensesTransformer()
        self.loader = ExpensesLoader(engine)  # SQLAlchemy engine now


    def extract(self) -> pd.DataFrame | None:
        try:
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
