"""Pipeline: Inventory
- Extracts inventory sheet, transforms and loads into DB.
- Follows the ETL pipeline contract: extract -> transform -> load.
"""

import logging
from pipelines.base_pipeline import ETLPipelineBase
from extract.google_sheet_extractor import GoogleSheetExtractor
from transform.inventory_transformer import InventoryTransformer
from loaders.inventory_loader import InventoryLoader
from config.settings import GOOGLE_SHEETS
import pandas as pd

logger = logging.getLogger(__name__)


class InventoryPipeline(ETLPipelineBase):
    def __init__(self, sheet_url: str, engine):
        super().__init__()
        self.extractor = GoogleSheetExtractor(service_account_path=str(GOOGLE_SHEETS.service_account_file))
        self.extractor.connect(sheet_url)
        self.transformer = InventoryTransformer()
        self.loader = InventoryLoader(engine)

    def extract(self) -> pd.DataFrame | None:
        try:
            return self.extractor.extract_sheet(index=GOOGLE_SHEETS.sheet_index_map["inventory"])
        except Exception as exc:
            logger.error(f"InventoryPipeline extract failed: {exc}")
            return None

    def transform(self, extracted: pd.DataFrame | None) -> pd.DataFrame | None:
        try:
            return self.transformer.transform(extracted)
        except Exception as exc:
            logger.error(f"InventoryPipeline transform failed: {exc}")
            return None

    def load(self, transformed: pd.DataFrame | None) -> int | None:
        try:
            return self.loader.load(transformed)
        except Exception as exc:
            logger.error(f"InventoryPipeline load failed: {exc}")
            return None
