from copy import deepcopy

from marshmallow import fields, validate, RAISE, validates_schema
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException

from awsglue.context import GlueContext
from jeda.models.constants import SerializationFormats
from ...exceptions import BaseException
from jeda.models.job_output import OutputTarget
from jeda.models.job_output import OutputTargetSchema
from typing import Iterable,List
from joblib.constant.constants import TechnicalColumn_scd4current
from pyspark.sql.types import StringType
from joblib.constant import common_values as VAL
from joblib.constant.curated_table import curated_table as TBL


class IcebergSCD4CurrentWriterOutputTargetSchema(OutputTargetSchema):
    class Meta:
        unknown = RAISE

    path = fields.String(required=False)
    primary_columns = fields.List(
        fields.String(), required=True, validate=validate.Length(min=1)
    )
    catalog_name = fields.String(required=True)
    db_name = fields.String(required=True)
    tbl_name = fields.String(required=True)
    src_stms = fields.List(
        fields.String(), required=False, validate=validate.Length(min=1),load_default = None
    )
    #ctas table if not exist
    # path = fields.String(required=True)
    # partition_columns = fields.List(
    #     fields.String(), required=True, validate=validate.Length(min=1)
    # )

    def validate(self, data, **kwargs):
        if data.get("serde", {}).get("format", "") != SerializationFormats.iceberg:
            raise BaseException(
                f"Only {SerializationFormats.iceberg} format is supported"
            )


class IcebergSCD4CurrentWriter:
    """
        'scd4current writer' process for the input data to be full data has effect at the etl date
        require column: rec_st, ppn_dt : etl date,
        src_stms : columns help define data partitioning use for case of multiple sources impact to 1 target table 
    """
    def __init__(
        self,
        glue_context: GlueContext,
        spark: SparkSession,
        df: DataFrame,
        target: OutputTarget,
        process_date: str,
    ):
        # Re-validate config
        config = deepcopy(target._output_target_dict)
        config = dict((k, v) for k, v in config.items() if v is not None)
        marshalled = IcebergSCD4CurrentWriterOutputTargetSchema().load(config)
        self.spark = spark
        self.glue_context = glue_context
        self.df = df
        self.target = target
        self.target.primary_columns = marshalled["primary_columns"]
        self.primary_columns: List[str] = self.target.primary_columns
        self.catalog_name = marshalled["catalog_name"]
        self.tbl_name = marshalled["tbl_name"]
        self.db_name = marshalled["db_name"]
        self.process_date = process_date
        self.target.src_stms = marshalled["src_stms"]
        self.src_stms: List[str] = self.target.src_stms
        #ctas table if not exist
        # self.path = marshalled["path"]
        # self.target.partition_columns = marshalled["partition_columns"]

    @property
    def technical_columns(self) -> Iterable[str]:
        return [
            TechnicalColumn_scd4current.RECORD_STATUS,
            TechnicalColumn_scd4current.PPN_DT,
        ]
    
    def merge_data(self) -> DataFrame:
        try:
            #read table as dataframe
            df_merge = self.df
            df_merge.persist()
            print("===============output_df_scd4current count:", df_merge.count())
            #ctas table if not exist
            #ctas_if_not_exist(self.spark,self.catalog_name,self.db_name,self.tbl_name,self.partition_columns,getattr(TBL,self.tbl_name),  self.path)
            
            df_target = self.spark.table("{0}.{1}.{2}".format(
                self.catalog_name,self.db_name,self.tbl_name
            ))
            df_target = df_target.filter(F.col(TechnicalColumn_scd4current.RECORD_STATUS) == 1) 

            #condition check status
            conditions_ = [F.when(F.concat_ws("",df_merge[c].cast(StringType())) != F.concat_ws("",df_target[c].cast(StringType())), F.lit(c)).otherwise("") for c in df_merge.columns if
                c not in self.technical_columns]

            status = (
                F.when(df_merge[self.primary_columns[0]].isNull(), F.lit("deleted"))
                .when(
                    F.size(F.array_remove(F.array(*conditions_), "")) > 0, F.lit("updated")
                )
                .otherwise("unchanged")
            )

            select_expr = [
                *[
                    df_target[c].alias(c)
                    for c in df_merge.columns
                    if c not in self.technical_columns
                ],
                status.alias("status"),
            ]

            #check status current record
            if self.src_stms is not None:
                df_check_par = df_merge.select(*self.src_stms).distinct()    
                df_target = df_target.join(
                    df_check_par,
                    on=[df_target[pc] == df_check_par[pc]for pc in self.src_stms],
                    how="inner",
                ).select(df_target["*"])
            df_check_stt = df_target.join(
                df_merge,
                on=[df_target[pk] == df_merge[pk] for pk in self.primary_columns],
                how="left",
            ).select(*select_expr)

            #condition merge
            columns_key = self.target.primary_columns
            columns_key_s = []
            for column in columns_key:
                condition_key = "t." + column + " = " + "s." + column
                columns_key_s.append(condition_key)
            columns_key_s = " and ".join(columns_key_s)

            #update record delete
            df_delete = df_check_stt.alias('df_delete')
            df_delete=df_delete.filter(F.col("status")=="deleted")
            df_delete.createOrReplaceTempView("delete_tbl")
            query_expire = """
                UPDATE {0}.{1}.{2} as t
                SET rec_st = 0,ppn_dt = cast('{3}' as date)
                WHERE EXISTS (SELECT 1 FROM delete_tbl s WHERE {4})
            """.format(
                self.catalog_name,self.db_name,self.tbl_name,self.process_date,columns_key_s
            )
            print(query_expire)
            self.spark.sql(query_expire)

            # update & insert new data 
            new_df = (
                df_merge.join(
                    df_check_stt,
                    on=[df_check_stt[pk] == df_merge[pk] for pk in self.primary_columns],
                    how="left",
                )
                .filter(~df_check_stt.status.eqNullSafe("unchanged"))
                .select(df_merge["*"])
            )
            if df_target.count()>0:
                print("not empty")
                new_df = new_df.withColumn("ppn_dt",F.when(df_merge["ppn_dt"] != F.to_date(F.lit(self.process_date),VAL.GOLDEN_ZONE_DATE_FORMAT),F.to_date(F.lit(self.process_date),VAL.GOLDEN_ZONE_DATE_FORMAT)).otherwise(df_merge["ppn_dt"]))
            
            new_df.createOrReplaceTempView("new_tbl")
            query = """
                MERGE INTO {0}.{1}.{2} t
                USING (SELECT * FROM new_tbl) s
                ON {3}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """.format(
                self.catalog_name, self.db_name, self.tbl_name, columns_key_s
            )
            print(query)
            self.spark.sql(query)

        except AnalysisException as e:
            print(e)
            raise e

    def write(self):
        self.merge_data()