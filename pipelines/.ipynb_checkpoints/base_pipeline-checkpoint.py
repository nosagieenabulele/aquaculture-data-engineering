# etl/pipelines/base_pipeline.py

"""
Base class for all ETL pipelines.
Defines the standard interface: extract, transform, load.
Child classes must implement these methods.
"""

from abc import ABC, abstractmethod

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
        """Run the full ETL pipeline end-to-end"""
        extracted = self.extract()
        transformed = self.transform(extracted)
        result = self.load(transformed)
        return result
