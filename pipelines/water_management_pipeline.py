"""Pipeline: Water Management
- Extracts water management sheet, transforms and loads into DB.
- Follows extract -> transform -> load contract from ETL pipeline base.
"""

import logging
# use repo-local modules (remove 'etl.' prefix)
from pipelines.base_pipeline import ETLPipelineBase
from extract.google_sheet_extractor import GoogleSheetExtractor
from transform.water_management_transformer import WaterManagementTransformer
from loaders.water_management_loader import WaterManagementLoader
from config.settings import GOOGLE_SHEETS
import pandas as pd

logger = logging.getLogger(__name__)


class WaterManagementPipeline(ETLPipelineBase):

    def __init__(self, sheet_url: str, engine):
        super().__init__()
        # initialize extractor with service account and connect to sheet
        self.extractor = GoogleSheetExtractor(service_account_path=str(GOOGLE_SHEETS.service_account_file))
        self.extractor.connect(sheet_url)
        # transformer and loader instances
        self.transformer = WaterManagementTransformer()
        self.loader = WaterManagementLoader(engine)


    def extract(self) -> pd.DataFrame | None:
        try:
            # read configured worksheet index for water_management
            return self.extractor.extract_sheet(index=GOOGLE_SHEETS.sheet_index_map["water_management"])
        except Exception as exc:
            logger.error(f"WaterManagementPipeline extract failed: {exc}")
            return None

    def transform(self, extracted: pd.DataFrame | None) -> pd.DataFrame | None:
        try:
            # transform returns a cleaned DataFrame or None/empty
            return self.transformer.transform(extracted)
        except Exception as exc:
            logger.error(f"WaterManagementPipeline transform failed: {exc}")
            return None

    def load(self, transformed: pd.DataFrame | None) -> int | None:
        try:
            # loader.load should return number of rows inserted (or similar)
            return self.loader.load(transformed)
        except Exception as exc:
            logger.error(f"WaterManagementPipeline load failed: {exc}")
            return None
