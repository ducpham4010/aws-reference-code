from pyspark.sql import SparkSession

from da_ded.generic_modules.models.job_input import JobInput
from .base import BaseReader
from .catalog import CatalogReader
from .catalog_sql import CatalogSQLReader
from .raw import RawReader
from .t24_staging import T24StagingReader
from .jdbc import JdbcReader
from .xml_tax import XMLTaxReader
from .dynamodb import DynamoDBReader
from ..models.constants import ReaderType


def create_reader(job_input: JobInput, spark: SparkSession, glue_context, job_config=None) -> BaseReader:
    if job_input.reader_type == ReaderType.T24_STAGING:
        return T24StagingReader(job_input, spark, glue_context)
    elif job_input.reader_type == ReaderType.RAW:
        return RawReader(job_input, spark, glue_context)
    elif job_input.reader_type == ReaderType.CATALOG:
        return CatalogReader(job_input, spark, glue_context, job_config)
    elif job_input.reader_type == ReaderType.JDBC:
        return JdbcReader(job_input, spark, glue_context)
    elif job_input.reader_type == ReaderType.XML_TAX:
        return XMLTaxReader(job_input, spark, glue_context)
    elif job_input.reader_type == ReaderType.CATALOG_SQL:
        return CatalogSQLReader(job_input, spark, glue_context)
    elif job_input.reader_type == ReaderType.DYNAMO_LINEAGE:
        return DynamoDBReader(job_input, spark, glue_context)
    else:
        raise RuntimeError(f"Unsupported reader type {job_input.reader_type}")