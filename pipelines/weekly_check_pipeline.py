"""Pipeline: Weekly Check
- Extracts weekly check sheet, transforms and loads into DB.
- Follows extract -> transform -> load contract from ETL pipeline base.
"""

import logging
from pipelines.base_pipeline import ETLPipelineBase
from extract.google_sheet_extractor import GoogleSheetExtractor
from transform.weekly_check_transformer import WeeklyCheckTransformer
from loaders.weekly_check_loader import WeeklyCheckLoader
from config.settings import GOOGLE_SHEETS
import pandas as pd

logger = logging.getLogger(__name__)


class WeeklyCheckPipeline(ETLPipelineBase):

    def __init__(self, sheet_url: str, engine):
        super().__init__()
        self.extractor = GoogleSheetExtractor(service_account_path=str(GOOGLE_SHEETS.service_account_file))
        self.extractor.connect(sheet_url)
        self.transformer = WeeklyCheckTransformer()
        self.loader = WeeklyCheckLoader(engine)


    def extract(self) -> pd.DataFrame | None:
        try:
            # Assuming worksheet index used for weekly_check is configured in settings
            return self.extractor.extract_sheet(index=GOOGLE_SHEETS.sheet_index_map["weekly_check"])
        except Exception as exc:
            logger.error(f"WeeklyCheckPipeline extract failed: {exc}")
            return None

    def transform(self, extracted: pd.DataFrame | None) -> pd.DataFrame | None:
        try:
            return self.transformer.transform(extracted)
        except Exception as exc:
            logger.error(f"WeeklyCheckPipeline transform failed: {exc}")
            return None

    def load(self, transformed: pd.DataFrame | None) -> int | None:
        try:
            return self.loader.load(transformed)
        except Exception as exc:
            logger.error(f"WeeklyCheckPipeline load failed: {exc}")
            return None
