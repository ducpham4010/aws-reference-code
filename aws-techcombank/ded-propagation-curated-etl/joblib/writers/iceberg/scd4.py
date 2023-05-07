from copy import deepcopy

import pyspark.sql.functions as F
from awsglue.context import GlueContext
from jeda.models.constants import SerializationFormats
from jeda.models.job_output import OutputTargetSchema, OutputTarget
from marshmallow import fields, validate, RAISE
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.utils import AnalysisException

from joblib.exceptions import BaseException
from joblib.logger import logger


class IcebergSCD4WriterOutputTargetSchema(OutputTargetSchema):
    class Meta:
        unknown = RAISE

    path = fields.String(required=False)
    primary_columns = fields.List(
        fields.String(), required=True, validate=validate.Length(min=1)
    )
    catalog_name = fields.String(required=True)
    db_name = fields.String(required=True)
    tbl_name = fields.String(required=True)
    writer_type = fields.String(required=True)
    serde = fields.Dict(required=True)
    column_list = fields.List(fields.List(fields.String()), required=True)

    def validate(self, data, **kwargs):
        if data.get("serde", {}).get("format", "") != SerializationFormats.iceberg:
            raise BaseException(
                f"Only {SerializationFormats.iceberg} format is supported"
            )


class IcebergSCD4Writer:
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
        marshalled = IcebergSCD4WriterOutputTargetSchema().load(config)
        self.spark = spark
        self.glue_context = glue_context
        self.df = df
        self.target = target
        self.target.primary_columns = marshalled["primary_columns"]
        self.catalog_name = marshalled["catalog_name"]
        self.tbl_name = marshalled["tbl_name"]
        self.db_name = marshalled["db_name"]

    def merge_data_cob(self):
        try:
            df_merge = self.df

            deleted_recs_df = df_merge.where("ds_record_is_deleted == 1")
            active_recs_df = df_merge.where("ds_record_is_deleted == 0")

            # Process active data

            # Add insert_date to df_merge
            active_recs_df = (
                active_recs_df.withColumn("insert_date", F.current_timestamp())
                .withColumn("update_date", F.lit(None).cast(TimestampType()))
                .withColumn("rec_status", F.lit(1))
            )

            columns_key = self.target.primary_columns
            join_condition = []
            for column in columns_key:
                condition_key = "t." + column + " = " + "s." + column
                join_condition.append(condition_key)
            join_condition_str = " and ".join(join_condition)

            df_target = self.spark.sql(
                f"select * from {self.catalog_name}.{self.db_name}.{self.tbl_name} limit 10"
            )

            df_target_columns = df_target.columns
            set_clause = []
            metadata_columns = ["insert_date", "update_date", "rec_status"]

            for column in df_target_columns:
                if column not in metadata_columns:
                    key = "t." + column + " = " + "s." + column
                    set_clause.append(key)

            set_clause_str = " , ".join(set_clause)
            set_clause_str += ", t.update_date = current_timestamp()"

            active_recs_df.createOrReplaceTempView("merge_iceberg")
            query = """
                MERGE INTO {0}.{1}.{2} t
                USING (SELECT * FROM merge_iceberg) s
                ON {3}
                WHEN MATCHED THEN UPDATE SET {4}
                WHEN NOT MATCHED THEN INSERT *
            """.format(
                self.catalog_name,
                self.db_name,
                self.tbl_name,
                join_condition_str,
                set_clause_str,
            )
            self.spark.sql(query)

            # Process deleted data
            self.delete_inactive_records_cob(deleted_recs_df, join_condition_str)

        except AnalysisException as e:
            logger.info(e)
            raise

    def delete_inactive_records_cob(self, deleted_recs_df, join_condition_str):
        try:
            deleted_recs_df.createOrReplaceTempView("deleted_data")

            query = """
                MERGE INTO {0}.{1}.{2} t
                USING (SELECT * FROM deleted_data) s
                ON {3}
                WHEN MATCHED THEN UPDATE SET t.rec_status = 0, 
                t.update_date = current_timestamp()                
            """.format(
                self.catalog_name, self.db_name, self.tbl_name, join_condition_str
            )
            self.spark.sql(query)
        except AnalysisException as e:
            logger.info(e)
            raise

    def merge_data_cob_hist(self) -> DataFrame:
        try:
            df_merge = self.df

            deleted_recs_df = df_merge.where("ds_record_is_deleted == 1").drop(
                "ds_record_is_deleted"
            )

            active_recs_df = df_merge.where("ds_record_is_deleted == 0").drop(
                "ds_record_is_deleted"
            )

            # Process active data

            df_merge_column_list = df_merge.drop("ds_record_is_deleted").columns

            # Add columns eff_st_dt, eff_end_dt, rec_status to active_recs_df
            active_recs_df = (
                active_recs_df.withColumn("rec_status", F.expr("CAST(NULL AS int)"))
                .withColumn("eff_st_dt", F.current_timestamp())
                .withColumn("eff_end_dt", F.expr("CAST(NULL AS timestamp)"))
            )

            active_recs_df.createOrReplaceTempView("merge_iceberg")

            columns_key = self.target.primary_columns
            join_condition = []
            for column in columns_key:
                condition_key = "t." + column + " = " + "s." + column
                join_condition.append(condition_key)
            join_condition_str = " and ".join(join_condition)

            table_update_qry = f"""
            WITH table_to_update AS (
            SELECT t.{',t.'.join(df_merge_column_list)}, t.eff_st_dt, t.eff_end_dt, t.rec_status
            FROM {self.catalog_name}.{self.db_name}.customers__cob_hist AS t
            JOIN merge_iceberg AS s ON {join_condition_str}
            UNION
            SELECT {','.join(df_merge_column_list)}, eff_st_dt, eff_end_dt, rec_status
            FROM merge_iceberg
            ), 
            table_updated AS (
            SELECT *,
            LEAD(eff_st_dt) OVER (PARTITION BY customer_id ORDER BY eff_st_dt) AS eff_lead
            FROM table_to_update
            )
            SELECT {",".join(df_merge_column_list)},
            (CASE WHEN eff_lead IS NULL THEN 1 ELSE 0 END) AS rec_status,
            eff_st_dt,
            eff_lead AS eff_end_dt
            FROM table_updated
            ORDER BY customer_id
            """

            print("table_update_qry:", table_update_qry)
            merge_query = f"""MERGE INTO {self.catalog_name}.{self.db_name}.{self.tbl_name}_hist t  
            USING ({table_update_qry}) s 
            ON {join_condition_str} and t.eff_st_dt = s.eff_st_dt
            WHEN MATCHED THEN UPDATE SET * 
            WHEN NOT MATCHED THEN INSERT *"""

            print("merge_query:", merge_query)

            self.spark.sql(merge_query)

            # Process deleted records
            self.delete_inactive_records_cob_hist(deleted_recs_df, join_condition_str)

        except AnalysisException as e:
            logger.info(e)
            raise

    def delete_inactive_records_cob_hist(self, deleted_recs_df, join_condition_str):
        try:
            deleted_recs_df.createOrReplaceTempView("deleted_data")

            query = """
                MERGE INTO {0}.{1}.{2}_hist t
                USING (SELECT * FROM deleted_data) s
                ON {3} and t.rec_status = 1
                WHEN MATCHED THEN UPDATE SET t.rec_status = 0 , 
                t.eff_end_dt = current_timestamp()                
            """.format(
                self.catalog_name, self.db_name, self.tbl_name, join_condition_str
            )
            self.spark.sql(query)
        except AnalysisException as e:
            logger.info(e)
            raise

    def write(self):
        self.merge_data_cob()
        self.merge_data_cob_hist()