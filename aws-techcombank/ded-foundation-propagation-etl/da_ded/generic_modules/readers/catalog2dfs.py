import typing
from typing import Dict

from marshmallow import Schema, fields
from pyspark.sql import SparkSession, DataFrame

from .base import BaseReader
from ..models.job_input import GlueCatalog


class GlueCatalog2DataframsReaderConfig(Schema):
    reader_conf = fields.Dict(keys=fields.Str(), values=fields.Nested(GlueCatalog()))


class Catalog2DataFramesReaderV1(BaseReader):
    _conf = None

    def set_conf(self, reader_conf: typing.Dict):
        """set reader config"""
        self._conf = GlueCatalog2DataframsReaderConfig().load(reader_conf)

    def read(self):
        name2dfs = {}
        for catalog_table_name, from_catalog_config in self._conf.items():
            df = self.glue_context.create_dynamic_frame.from_catalog(
                database=from_catalog_config["database"],
                table_name=from_catalog_config["table"],
                push_down_predicate=from_catalog_config["push_down_predicate"] or "",
            ).toDF()
            name2dfs[catalog_table_name] = df
        return name2dfs


def read_dfs_from_catalog(source_config, glue_context, spark: SparkSession) -> Dict[str, DataFrame]:
    # TODO - no unit test for this, need integration test on cloud
    # TODO - passing job_input=None is kinda hacky here, a more general approach would be
    #  instead to have only reader type and reader config that type needed in the framework
    #  currently the JobInput has to contain params for all the reader type
    reader = Catalog2DataFramesReaderV1(job_input=None, spark=spark, glue_context=glue_context)
    reader.set_conf(source_config["reader_conf"])
    tablenames2dfs = reader.read()
    return tablenames2dfs