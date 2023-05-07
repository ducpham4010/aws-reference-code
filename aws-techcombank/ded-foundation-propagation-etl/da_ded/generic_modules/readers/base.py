from abc import ABC, abstractmethod

from pyspark.sql import SparkSession

from da_ded.generic_modules.models.job_input import JobInput


class BaseReader(ABC):
    def __init__(self, job_input: JobInput, spark: SparkSession, glue_context):
        self.job_input = job_input
        self.spark = spark
        self.glue_context = glue_context

    @abstractmethod
    def read(self):
        pass