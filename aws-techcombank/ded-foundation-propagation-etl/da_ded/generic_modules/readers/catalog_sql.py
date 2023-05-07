from pyspark.sql.session import SparkSession

from da_ded.data_veracity.models import JobInput
from da_ded.generic_modules.readers.base import BaseReader


class CatalogSQLReader(BaseReader):
    def __init__(self, job_input: JobInput, spark: SparkSession, glue_context):
        super().__init__(job_input, spark, glue_context)
        self.logger = glue_context.get_logger()

    def read(self):
        try:
            df = self.glue_context.sparkSession.sql(self.job_input.query).cache()
            yield df
        except Exception as exp:
            raise exp