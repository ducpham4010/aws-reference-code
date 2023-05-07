from marshmallow import Schema, fields, validate
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import trim, col, when, from_utc_timestamp
from pyspark.sql.utils import AnalysisException

from da_ded.utils import get_data_files_from_entry, get_matching_s3_keys

from .base import BaseReader
from ..models.constants import SerializationFormats, PartitionConfigs
from ..models.job_input import JobInput
from ..models.serde import Serde


class RawReaderConfigSchema(Schema):
    entry_file = fields.String(load_default=None)
    uri_list = fields.List(fields.String(), load_default=None)
    serde = fields.Dict(load_default=None)
    schema = fields.List(fields.Raw(), load_default=None)
    partition_by = fields.String(
        required=False,
        load_default=None,
        allow_none=True,
        validate=validate.OneOf(PartitionConfigs.items()),
    )


class RawReaderConfig:
    def __init__(self, a_dict: dict):
        self.entry_file = a_dict["entry_file"]
        self.uri_list = a_dict["uri_list"]
        self.serde = Serde.create(a_dict["serde"])
        self.schema = a_dict["schema"]
        self.partition_by = a_dict["partition_by"]


def parse_reader_config(job_input: JobInput) -> RawReaderConfig:
    if job_input.reader_config:
        # new config version
        config_dict = job_input.reader_config
        config_dict = RawReaderConfigSchema().load(config_dict)
        config = RawReaderConfig(config_dict)
    else:
        # old legacy config fields that are used and are in JobInput
        config = RawReaderConfig(job_input.__dict__)
    return config


class RawReader(BaseReader):
    """Read directly from S3 bucket"""

    def read(self):
        config = parse_reader_config(self.job_input)
        return self._read(self.spark, config)

    @classmethod
    def _read_csv(cls, spark: SparkSession, config: RawReaderConfig) -> DataFrame:
        if config.entry_file:
            data_files = get_data_files_from_entry(config.entry_file)
        else:
            data_files = config.uri_list
        df = spark.read.options(header="True", delimiter=",", nullValue="", nanValue="", inferSchema="False",).csv(
            data_files,
            encoding=config.serde.encoding,
            escape=config.serde.escape,
            quote=config.serde.quote,
            multiLine=config.serde.multi_line,
        )

        if config.entry_file:
            result = []
            for file in data_files:
                tmp = file.split("/")[-1].split(".")[0].split("-")
                result += [int(i) for i in tmp if i.isnumeric()]
            result.sort()
            if result:
                print(f"Number row in file name is {result[-1]}, not match number row dataframe {df.count()}")

        if config.schema is not None:
            keep_fields = [field["name"] for field in config.schema]
            df = df.select(*keep_fields)
            for field in config.schema:
                df = df.withColumn(field["name"], trim(df[field["name"]]))
                df = df.withColumn(
                    field["name"],
                    when(col(field["name"]) == "", None).otherwise(col(field["name"])),
                )
                df = df.withColumn(field["name"], df[field["name"]].cast(field["type"]))
                if "timestamp" == field["type"]:
                    # By default, cast method transform the timestamp to UTC+0, we need to convert back to UTC+7
                    df = df.withColumn(field["name"], from_utc_timestamp(df[field["name"]], "Asia/Ho_Chi_Minh"))
        return df

    @classmethod
    def _read_parquet(cls, spark: SparkSession, config: RawReaderConfig):
        if config.partition_by == PartitionConfigs.DAILY:
            # job_input is parquet S3 with daily partitioning, need yield data read from
            all_partitions = get_matching_s3_keys(config.uri_list[0])
            for each_partition_dir in all_partitions:
                yield spark.read.parquet(each_partition_dir)
            # TODO: options for parquet files
        else:
            try:
                df = spark.read.parquet(config.uri_list[0])
                yield df
            except AnalysisException as e:
                ignore_messages = [
                    "Path does not exist",
                    "Unable to infer schema for Parquet. It must be specified manually",
                ]

                if all([message not in str(e) for message in ignore_messages]):
                    raise e

    @classmethod
    def _read_json(cls, spark: SparkSession, config: RawReaderConfig):
        # Firehose Json Output, Json raw files in s3 bucket,..
        json_serde = config.serde
        df = spark.read.json(
            path=config.uri_list,
            multiLine=json_serde.multiLine,
            dateFormat=json_serde.dateFormat,
            timestampFormat=json_serde.timestampFormat,
            encoding=json_serde.encoding,
            lineSep=json_serde.lineSep,
            samplingRatio=json_serde.samplingRatio,
            dropFieldIfAllNull=json_serde.dropFieldIfAllNull,
        )
        return df

    @classmethod
    def _read(cls, spark: SparkSession, config: RawReaderConfig):
        """Read with config in job_input"""
        input_format = config.serde.format
        if input_format == SerializationFormats.CSV:
            yield cls._read_csv(spark, config)
        elif input_format == SerializationFormats.PARQUET:
            yield from cls._read_parquet(spark, config)
        elif input_format == SerializationFormats.JSON:
            yield cls._read_json(spark, config)

        else:
            raise ValueError(f"Not supported reading {input_format}, config {config}.")