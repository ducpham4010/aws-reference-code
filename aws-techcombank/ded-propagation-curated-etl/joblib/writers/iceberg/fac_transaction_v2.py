from jeda.writers.fact_v2 import FactV2Writer
from joblib.util.utils import (
    execute_writer,
)
from joblib.writers.iceberg.fact_periodic import (
    IcebergFactPeriodicWriter as IcgWriterFD,
)
from copy import deepcopy

from marshmallow import fields, validate, EXCLUDE, validates_schema
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

from jeda.models.constants import SerializationFormats, DsColumn, RecordChangeType
from jeda.models.job_output import OutputTarget
from jeda.models.job_output import OutputTargetSchema
from jeda.util.logger import logger


class FactV2WriterOutputTargetSchema(OutputTargetSchema):
    """
    Ref:
        mode='DEDUP': https://confluence.techcombank.com.vn/display/DataEngineering/How+to+handle+fact+table
        mode='MIGRATION_TYPE_9': https://confluence.techcombank.com.vn/pages/viewpage.action?pageId=165626101
    """

    class Meta:
        unknown = EXCLUDE

    mode = fields.String(
        load_default="DEDUP", validate=validate.OneOf(["DEDUP", "MIGRATION_TYPE_9"])
    )
    path = fields.String(required=True)
    partition_by = fields.String(required=True)
    primary_columns = fields.List(
        fields.String(), required=True, validate=validate.Length(min=1)
    )
    field_to_get_max = fields.String(load_default=None)
    sort_by_record_change_type = fields.Boolean(load_default=True)

    @validates_schema(skip_on_field_errors=True)
    def validate(self, data, **kwargs):
        if data.get("serde", {}).get("format", "") != SerializationFormats.iceberg:
            raise Exception(f"Only {SerializationFormats.iceberg} format is supported")
        if data["mode"] == "MIGRATION_TYPE_9" and data["field_to_get_max"] is None:
            raise Exception(
                f"`field_to_get_max` parameter is required when mode=MIGRATION_TYPE_9"
            )


class FactV2WriterIceberg:
    def __init__(self, glue_context, spark, df, target):
        self.glue_context = glue_context
        # Re-validate config
        config = deepcopy(target._output_target_dict)
        config = dict((k, v) for k, v in config.items() if v is not None)
        marshalled = FactV2WriterOutputTargetSchema().load(config)

        self.spark = spark
        self.df = df
        self.target = target
        self.target.mode = marshalled["mode"]
        self.target.primary_columns = marshalled["primary_columns"]
        if self.target.partition_by not in self.target.primary_columns:
            self.target.primary_columns.append(self.target.partition_by)
        self.target.field_to_get_max = marshalled["field_to_get_max"]
        self.target.sort_by_record_change_type = marshalled[
            "sort_by_record_change_type"
        ]

    def _get_existing_df(self) -> DataFrame:
        try:
            partition_col = self.target.partition_by
            df = self.df.withColumn(
                partition_col, self.df[partition_col].cast("string")
            )
            partition_values = [
                row[partition_col]
                for row in df.select(partition_col).distinct().collect()
            ]
            logger.info(
                f"Fist 10 partition values of current DF: {partition_values[:10]}"
            )

            existing_df = self.spark.read.parquet(self.target.path).filter(
                F.col(self.target.partition_by).isin(*partition_values)
            )
            # Print execution plan to know if pushdown predicate work
            # Ref: https://stackoverflow.com/a/60795708
            # logger.info(existing_df.explain(True))
            return existing_df
        except AnalysisException as e:
            logger.info(
                f"Cannot read from path {self.target.path}, probably path does not exist"
            )
            logger.info(e)
            return None

    def _get_union_df_dedup_mode(self, existing_df: DataFrame) -> DataFrame:
        self.df = self.df.dropDuplicates(self.target.primary_columns)
        if not existing_df:
            return self.df
        df = existing_df.join(self.df, self.target.primary_columns, "left_anti")
        return df.unionByName(self.df)

    def _get_union_df_migration_type_9_mode(self, existing_df: DataFrame) -> DataFrame:
        if not existing_df:
            df = self.df
        else:
            df = self.df.unionByName(existing_df)

        if self.target.sort_by_record_change_type:
            df = df.withColumn(
                "_record_change_type_order",
                F.when(
                    F.col(DsColumn.RECORD_CHANGE_TYPE) == RecordChangeType.DWH_1TM, 1
                )
                .when(F.col(DsColumn.RECORD_CHANGE_TYPE) == RecordChangeType.T24_1TM, 2)
                .when(
                    F.col(DsColumn.RECORD_CHANGE_TYPE).isin(
                        [
                            RecordChangeType.CDC_DELETE,
                            RecordChangeType.CDC_INSERT,
                            RecordChangeType.CDC_UPDATE,
                        ]
                    ),
                    3,
                )
                .otherwise(0),
            )
            df.show()
            w = (
                Window()
                .partitionBy(self.target.primary_columns)
                .orderBy(
                    F.col(self.target.field_to_get_max).desc(),
                    F.col("_record_change_type_order").desc(),
                )
            )
        else:
            w = (
                Window()
                .partitionBy(self.target.primary_columns)
                .orderBy(F.col(self.target.field_to_get_max).desc())
            )

        return (
            df.withColumn("_row_number", F.row_number().over(w))
            .where(F.col("_row_number") == 1)
            .drop("_row_number", "_record_change_type_order")
        )

    def _get_union_df(self) -> DataFrame:
        """Union current DF with partitions X of existing DF if X is also in current DF
        Old records (based on primary_columns) is dropped
        """
        existing_df = self._get_existing_df()
        if self.target.mode == "DEDUP":
            return self._get_union_df_dedup_mode(existing_df)
        elif self.target.mode == "MIGRATION_TYPE_9":
            return self._get_union_df_migration_type_9_mode(existing_df)
        # Should never reach here
        raise

    def _drop_technical_column(self):
        technical_cols = set(
            [
                DsColumn.RECORD_UPDATED_TIMESTAMP,
                DsColumn.PROCESS_DATE,
                DsColumn.PARTITION_RECID,
                DsColumn.RECORD_STATUS,
            ]
        )
        df_cols = set(self.df.columns)

        overlap_cols = technical_cols.intersection(df_cols)
        if overlap_cols:
            self.df = self.df.drop(*overlap_cols)

    def write_to_iceberg(self):
        self._drop_technical_column()
        df = self._get_union_df()
        writer = IcgWriterFD(
            self.glue_context,
            self.spark,
            df,
            self.target,
        )
        execute_writer(self.spark, df, self.target, writer)