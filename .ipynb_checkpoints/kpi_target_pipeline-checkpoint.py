# etl/pipelines/kpi_target_pipeline.py

import logging
from etl.pipelines.base_pipeline import ETLPipelineBase
from etl.extract.google_sheet_extractor import GoogleSheetExtractor
from etl.transform.kpi_target_transformer import KPITargetTransformer
from etl.loaders.kpi_target_loader import KPITargetLoader
from etl.config.settings import GOOGLE_SHEETS
import pandas as pd

logger = logging.getLogger(__name__)


class KPITargetPipeline(ETLPipelineBase):

    def __init__(self, sheet_url: str, engine):
        super().__init__()
        self.extractor = GoogleSheetExtractor(service_account_path=str(GOOGLE_SHEETS.service_account_file))
        self.extractor.connect(sheet_url)
        self.transformer = KPITargetTransformer()
        self.loader = KPITargetLoader(engine)

    def extract(self) -> pd.DataFrame | None:
        try:
            return self.extractor.extract_sheet(index=GOOGLE_SHEETS.sheet_index_map["kpi_target"])
        except Exception as exc:
            logger.error(f"KPITargetPipeline extract failed: {exc}")
            return None

    def transform(self, extracted: pd.DataFrame | None) -> pd.DataFrame | None:
        try:
            return self.transformer.transform(extracted)
        except Exception as exc:
            logger.error(f"KPITargetPipeline transform failed: {exc}")
            return None

    def load(self, transformed: pd.DataFrame | None) -> int | None:
        try:
            return self.loader.load(transformed)
        except Exception as exc:
            logger.error(f"KPITargetPipeline load failed: {exc}")
            return None
