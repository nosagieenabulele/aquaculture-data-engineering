# etl/pipelines/water_management_pipeline.py

import logging
from etl.pipelines.base_pipeline import ETLPipelineBase
from etl.extract.google_sheet_extractor import GoogleSheetExtractor
from etl.transform.water_management_transformer import WaterManagementTransformer
from etl.loaders.water_management_loader import WaterManagementLoader
from etl.config.settings import GOOGLE_SHEETS
import pandas as pd

logger = logging.getLogger(__name__)


class WaterManagementPipeline(ETLPipelineBase):

    def __init__(self, sheet_url: str, engine):
        super().__init__()
        self.extractor = GoogleSheetExtractor(service_account_path=str(GOOGLE_SHEETS.service_account_file))
        self.extractor.connect(sheet_url)
        self.transformer = WaterManagementTransformer()
        self.loader = WaterManagementLoader(engine)


    def extract(self) -> pd.DataFrame | None:
        try:
            # Assume worksheet index 5 contains water management records
            return self.extractor.extract_sheet(index=GOOGLE_SHEETS.sheet_index_map["water_management"])
        except Exception as exc:
            logger.error(f"WaterManagementPipeline extract failed: {exc}")
            return None

    def transform(self, extracted: pd.DataFrame | None) -> pd.DataFrame | None:
        try:
            return self.transformer.transform(extracted)
        except Exception as exc:
            logger.error(f"WaterManagementPipeline transform failed: {exc}")
            return None

    def load(self, transformed: pd.DataFrame | None) -> int | None:
        try:
            return self.loader.load(transformed)
        except Exception as exc:
            logger.error(f"WaterManagementPipeline load failed: {exc}")
            return None
