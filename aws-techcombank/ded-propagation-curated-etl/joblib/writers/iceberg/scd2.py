from copy import deepcopy

from marshmallow import fields, validate, RAISE, validates_schema
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException

from pyspark.sql import DataFrame, functions as  SparkSession
from awsglue.context import GlueContext
from jeda.models.constants import SerializationFormats
from ...exceptions import BaseException
from jeda.models.job_output import OutputTarget
from jeda.models.job_output import OutputTargetSchema
from typing import Iterable,List
from joblib.constant.constants import TechnicalColumn_scd2
from pyspark.sql.types import StringType
from joblib.constant import common_values as VAL

class IcebergSCD2WriterOutputTargetSchema(OutputTargetSchema):
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
    
    options = fields.Dict(required=False, load_default=None, allow_none=True)

    def validate(self, data, **kwargs):
        if data.get("serde", {}).get("format", "") != SerializationFormats.iceberg:
            raise BaseException(
                f"Only {SerializationFormats.iceberg} format is supported"
            )


class IcebergSCD2Writer:
    """
        'SCD2 writer' process for the input data to be full data has effect at the process_date
    """
    def __init__(
        self,
        glue_context: GlueContext,
        spark: SparkSession,
        df: DataFrame,
        target: OutputTarget,
        process_date: str
    ):
        # Re-validate config
        config = deepcopy(target._output_target_dict)
        config = dict((k, v) for k, v in config.items() if v is not None)
        marshalled = IcebergSCD2WriterOutputTargetSchema().load(config)
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

    @property
    def technical_columns(self) -> Iterable[str]:
        return [
            TechnicalColumn_scd2.EFFECTIVE_DATE,
            TechnicalColumn_scd2.END_DATE,
            TechnicalColumn_scd2.RECORD_STATUS,
            TechnicalColumn_scd2.PPN_DT,
        ]
    
    def merge_data(self) -> DataFrame:
        """
            require column: eff_dt, end_dt, rec_st, ppn_dt : etl date,
            src_stms : columns help define data partitioning use for case of multiple sources impact to 1 target table 
        """
        try:
            #read table as dataframe
            df_merge = self.df
            df_merge.persist()
            print("===============output_df_scd4current count:", df_merge.count())
            df_target = self.spark.table("{0}.{1}.{2}".format(
                self.catalog_name,self.db_name,self.tbl_name
            ))
            df_target = df_target.filter(
                (F.to_date(F.col(TechnicalColumn_scd2.END_DATE),VAL.GOLDEN_ZONE_DATE_FORMAT) > self.process_date) & 
                (F.to_date(F.col(TechnicalColumn_scd2.EFFECTIVE_DATE),VAL.GOLDEN_ZONE_DATE_FORMAT) <= self.process_date))

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

            #update eff to date of record delete,expire
            
            df_delete = df_check_stt.alias('df_delete')
            df_delete=df_delete.filter(F.col("status").isin("deleted","updated"))
            df_delete.createOrReplaceTempView("delete_tbl")
            query_updatedelete  = """
                MERGE INTO {0}.{1}.{2} t
                USING (SELECT * FROM delete_tbl) s
                ON {3} and t.end_dt = cast('3000-11-08' as date)
                WHEN MATCHED THEN UPDATE SET end_dt = cast('{4}' as date), rec_st = 0,ppn_dt = cast('{4}' as date)
            """.format(
                self.catalog_name,self.db_name,self.tbl_name,columns_key_s,self.process_date
            )
            print(query_updatedelete)
            self.spark.sql(query_updatedelete)

            # insert new data and data update
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
                new_df = new_df.withColumn("eff_dt",F.when(df_merge["eff_dt"] != F.to_date(F.lit(self.process_date),VAL.GOLDEN_ZONE_DATE_FORMAT),F.to_date(F.lit(self.process_date),VAL.GOLDEN_ZONE_DATE_FORMAT)).otherwise(df_merge["eff_dt"]))\
                    .withColumn("ppn_dt",F.when(df_merge["ppn_dt"] != F.to_date(F.lit(self.process_date),VAL.GOLDEN_ZONE_DATE_FORMAT),F.to_date(F.lit(self.process_date),VAL.GOLDEN_ZONE_DATE_FORMAT)).otherwise(df_merge["ppn_dt"]))
            
            new_df.createOrReplaceTempView("new_tbl")
            query_insert_new_update = """
                INSERT INTO {0}.{1}.{2}
                SELECT * FROM new_tbl
            """.format(
                self.catalog_name,self.db_name,self.tbl_name
            )
            print(query_insert_new_update)
            self.spark.sql(query_insert_new_update)
            
        except AnalysisException as e:
            print(e)
            raise e

    def write(self):
        self.merge_data()