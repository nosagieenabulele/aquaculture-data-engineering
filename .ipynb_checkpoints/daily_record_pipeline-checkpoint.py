import logging
from etl.pipelines.base_pipeline import ETLPipelineBase
from etl.extract.google_sheet_extractor import GoogleSheetExtractor
from etl.transform.daily_record_transformer import DailyRecordTransformer
from etl.loaders.daily_record_loader import DailyRecordLoader
from etl.config.settings import GOOGLE_SHEETS

logger = logging.getLogger(__name__)

class DailyRecordPipeline(ETLPipelineBase):

    def __init__(self, sheet_url: str, engine):
        super().__init__()
        self.extractor = GoogleSheetExtractor(service_account_path=str(GOOGLE_SHEETS.service_account_file))
        self.extractor.connect(sheet_url)
        self.transformer = DailyRecordTransformer()
        self.loader = DailyRecordLoader(engine)  # SQLAlchemy engine now

    def extract(self):
        try:
            return self.extractor.extract_sheet(index=GOOGLE_SHEETS.sheet_index_map["daily_record"])
        except Exception as exc:
            logger.error(f"DailyRecordPipeline extract failed: {exc}")
            return None

    def transform(self, extracted):
        try:
            return self.transformer.transform(extracted)
        except Exception as exc:
            logger.error(f"DailyRecordPipeline transform failed: {exc}")
            return None

    def load(self, transformed):
        try:
            return self.loader.load(transformed)
        except Exception as exc:
            logger.error(f"DailyRecordPipeline load failed: {exc}")
            return None
