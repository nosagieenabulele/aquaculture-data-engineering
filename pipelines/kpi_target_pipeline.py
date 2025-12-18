# etl/pipelines/kpi_target_pipeline.py

"""Pipeline: KPI Target
- Orchestrates extract -> transform -> load for KPI target worksheet.
- Uses GoogleSheetExtractor, KPITargetTransformer, and KPITargetLoader.
"""

import logging
from pipelines.base_pipeline import ETLPipelineBase
from extract.google_sheet_extractor import GoogleSheetExtractor
from transform.kpi_target_transformer import KPITargetTransformer
from loaders.kpi_target_loader import KPITargetLoader
from config.settings import GOOGLE_SHEETS
import pandas as pd

logger = logging.getLogger(__name__)


class KPITargetPipeline(ETLPipelineBase):

    def __init__(self, sheet_url: str, engine):
        super().__init__()
        # initialize extractor with service account and open the spreadsheet
        self.extractor = GoogleSheetExtractor(service_account_path=str(GOOGLE_SHEETS.service_account_file))
        self.extractor.connect(sheet_url)
        # transformer and loader instances
        self.transformer = KPITargetTransformer()
        self.loader = KPITargetLoader(engine)

    def extract(self) -> pd.DataFrame | None:
        try:
            # read the configured worksheet index for KPI targets
            return self.extractor.extract_sheet(index=GOOGLE_SHEETS.sheet_index_map["kpi_target"])
        except Exception as exc:
            logger.error(f"KPITargetPipeline extract failed: {exc}")
            return None

    def transform(self, extracted: pd.DataFrame | None) -> pd.DataFrame | None:
        try:
            # transformer returns cleaned DataFrame or empty DataFrame on no data
            return self.transformer.transform(extracted)
        except Exception as exc:
            logger.error(f"KPITargetPipeline transform failed: {exc}")
            return None

    def load(self, transformed: pd.DataFrame | None) -> int | None:
        try:
            # loader.load should return number of rows inserted or None on failure
            return self.loader.load(transformed)
        except Exception as exc:
            logger.error(f"KPITargetPipeline load failed: {exc}")
            return None
