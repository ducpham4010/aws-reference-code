from copy import deepcopy

from marshmallow import fields, validate, RAISE, validates_schema, EXCLUDE
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException

from awsglue.context import GlueContext
from jeda.models.constants import SerializationFormats
from ...exceptions import BaseException
from jeda.models.job_output import OutputTarget
from jeda.models.job_output import OutputTargetSchema
from joblib.constant.common_values import COMMON_BLANK


class IcebergFactPeriodicOutputTargetSchema(OutputTargetSchema):
    class Meta:
        unknown = EXCLUDE

    path = fields.String(required=False)
    # partition_columns = fields.List(
    #     fields.String(), required=True, validate=validate.Length(min=1)
    # )
    catalog_name = fields.String(required=True)
    db_name = fields.String(required=True)
    tbl_name = fields.String(required=True)

    def validate(self, data, **kwargs):
        if data.get("serde", {}).get("format", "") != SerializationFormats.iceberg:
            raise BaseException(
                f"Only {SerializationFormats.iceberg} format is supported"
            )


class IcebergFactPeriodicWriter:
    def __init__(
            self,
            glue_context: GlueContext,
            spark: SparkSession,
            df: DataFrame,
            target: OutputTarget,
    ):
        # Re-validate config
        config = deepcopy(target._output_target_dict)
        config = dict((k, v) for k, v in config.items() if v is not None)
        marshalled = IcebergFactPeriodicOutputTargetSchema().load(config)
        self.spark = spark
        self.glue_context = glue_context
        self.df = df
        self.target = target
        self.catalog_name = marshalled["catalog_name"]
        self.tbl_name = marshalled["tbl_name"]
        self.db_name = marshalled["db_name"]
        # self.target.partition_columns = marshalled["partition_columns"]

    def write_fact_periodic(self) -> DataFrame:
        #Partition information is required on the target table, if dont have information, writer will overwrite full data
        try:
            #read df source, df target
            df_src = self.df
            # df_src.persist()
            df_src.writeTo("{0}.{1}.{2}".format(
                 self.catalog_name,self.db_name,self.tbl_name
             )).overwritePartitions()
        except AnalysisException:
            raise AnalysisException(f"SQL Error")

    def write(self):
        self.write_fact_periodic()