# pipelines/base_pipeline.py

"""
Base class for all ETL pipelines.
Defines the standard interface: extract, transform, load.
Child classes must implement these methods.
"""

from abc import ABC, abstractmethod
import logging

logger = logging.getLogger(__name__)

class ETLPipelineBase(ABC):

    @abstractmethod
    def extract(self):
        """Extract data from source (Google Sheets, CSV, etc.)"""
        pass

    @abstractmethod
    def transform(self, extracted):
        """Transform extracted data into cleaned/processed format"""
        pass

    @abstractmethod
    def load(self, transformed):
        """Load transformed data into the target (database, file, etc.)"""
        pass

def run(self):
        try:
            logger.info(f"Starting pipeline: {self.__class__.__name__}")
            extracted = self.extract()
            if extracted is None: return None

            transformed = self.transform(extracted)
            
            # --- ADD THIS TEMPORARY INSPECTOR ---
            if transformed is not None:
                print(f"\n🔍 INSPECTION: {self.__class__.__name__}")
                print(f"Columns actually produced: {transformed.columns.tolist()}")
                print("-" * 30)
            # ------------------------------------

            if transformed is None: return None

            result = self.load(transformed)
            return result
        except Exception as exc:
            logger.exception("Pipeline run failed")
            raise