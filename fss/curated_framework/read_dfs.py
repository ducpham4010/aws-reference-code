"""
input = {
    "Inputs": 
    {
        "glue_catalog": {
                "database": "tcb-ps5-data-sit-golden_ods",
                "table": "customers",
                "partition_column" : "customer_id",
                "transformation_ctx" : [],
                "additional_options" : {}
                },
        "reader_type" : "catalog",
        "query": ""
    },
    "Outputs" : 
    {
        "catalog_name": "glue_catalog",
        "tbl_name": "t_customers",
        "db_name": "`tcb-sit-golden_curated`",
        "writer_type": "CUSTOM",
        "serde": {
            "format": "iceberg"
        },
        "primary_columns": [
        "customer_id",
        "src_stm_id"
        ],
        "src_stms": ["src_stm_id"]
    },
    "Params" : {"data_date" : "2022-04-24"}
}

"""


from pyspark.sql.session import SparkSession
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark import SparkContext
from pyspark.sql import DataFrame
from awsglue.job import Job
import sys
import json

from copy import deepcopy
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException
from pyspark.sql import DataFrame, functions as  SparkSession
from pyspark.sql.types import StringType,IntegerType
from awsglue.context import GlueContext
from typing import Iterable,List
from pyspark.sql.types import StringType

class CatalogReader():
    def __init__(self, job_input: dict, job_output: dict, spark: SparkSession, glue_context, job_config=None):
        self.job_config = job_config
        self.job_output = job_output
        self.job_input = job_input
        self.glue_context = glue_context
        self.spark = spark

    def read(self, predicate_value=None):
        reader_type = self.job_input.get("reader_type")
        catalog_name = self.job_output.get("catalog_name")
        db_name = self.job_output.get("db_name")
        tbl_name = self.job_output.get("tbl_name")
        if reader_type == 'catalog':
            try: 
                database = self.job_input.get("glue_catalog").get("database")
                table = self.job_input.get("glue_catalog").get("table")
                # partition_column = self.job_input.get("glue_catalog").get("partition_column")
                # push_down_predicate = f"{partition_column} != '{predicate_value}'"
                transformation_ctx = 'transformation_ctx'
                additional_options =  {}

                print(f"Data frame from : {database}.{table}")

                df = self.glue_context.create_dynamic_frame.from_catalog(
                    database=database,
                    table_name=table,
                    transformation_ctx=transformation_ctx,
                    # push_down_predicate=push_down_predicate,
                    additional_options=additional_options
                ).toDF()

                df_target = spark.table("{0}.{1}.{2}".format(catalog_name,db_name,tbl_name))
                return {"df_merge" :df, "df_target":df_target}
            except Exception as exp:
                print(exp)
                raise exp
        elif reader_type == 'catalog_sql':
            try:
                df = self.spark.sql(self.job_input.query)
                df_target = spark.table("{0}.{1}.{2}".format(catalog_name,db_name,tbl_name))
                return {"df_merge" :df, "df_target":df_target}
            except Exception as exp:
                print(exp)
                raise exp
        else :
            print(f"Unsupport reader type {reader_type} now")

class IcebergSCD2Writer:
    """
        'SCD2 writer' process for the input data to be full data has effect at the process_date
    """
    def __init__(
        self,
        glue_context: GlueContext,
        spark: SparkSession,
        df: DataFrame,
        df_target: DataFrame,
        job_output: dict,
        process_date: str
    ):
        # Re-validate config
        job_output = deepcopy(job_output)
        self.job_output = dict((k, v) for k, v in job_output.items() if v is not None)
        self.spark = spark
        self.glue_context = glue_context
        self.df = df
        self.df_target = df_target
        self.primary_columns = self.job_output.get("primary_columns")
        self.catalog_name = self.job_output.get("catalog_name")
        self.tbl_name = self.job_output.get("tbl_name")
        self.db_name = self.job_output.get("db_name")
        self.process_date = process_date
        self.src_stms = self.job_output.get("src_stms")
    @property
    def technical_columns(self) -> Iterable[str]:
        return [
            "eff_dt",
            "end_dt",
            "rec_st",
            "ppn_dt",
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
            print("===============output_df_count:", df_merge.count())
            df_target = self.df_target
            df_target = df_target.filter(
                (F.to_date(F.col("end_dt"),"yyyy-MM-dd") > self.process_date) & 
                (F.to_date(F.col("eff_dt"),"yyyy-MM-dd") <= self.process_date))

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
            columns_key = self.primary_columns
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
                new_df = new_df.withColumn("eff_dt",F.when(df_merge["eff_dt"] != F.to_date(F.lit(self.process_date),"yyyy-MM-dd"),F.to_date(F.lit(self.process_date),"yyyy-MM-dd")).otherwise(df_merge["eff_dt"]))\
                    .withColumn("ppn_dt",F.when(df_merge["ppn_dt"] != F.to_date(F.lit(self.process_date),"yyyy-MM-dd"),F.to_date(F.lit(self.process_date),"yyyy-MM-dd")).otherwise(df_merge["ppn_dt"]))
            
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

arguments = sys.argv
args = getResolvedOptions(arguments, ["JOB_NAME", "Inputs", "Outputs", "Params"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

input_data_str = args.get("Inputs")
job_input = json.loads(input_data_str)

output_data_str = args.get("Outputs")
job_output = json.loads(output_data_str)

params_str = args.get("Params")
params = json.loads(params_str)
process_date = params.get('data_date')

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


reader = CatalogReader(job_input, job_output, spark, glue_context, None)
df = reader.read().get("df_merge")
print("Data frame merge : ")
df.show()

df_target = reader.read().get("df_target")
print("Data frame target : ")
df_target.show()


df_customers = df.withColumn(
                "customer_id",
                df['customer_id']
        ).withColumn(
                "fn",
                df['fn'].cast(StringType())
        ).withColumn(
                "active",
                df['active'].cast(StringType())
        ).withColumn(
                "club_member_status",
                df['club_member_status'].cast(StringType())
        ).withColumn(
                "fashion_news_frequency",
                df['fashion_news_frequency'].cast(StringType())
        ).withColumn(
                "eff_dt",
                F.to_date(F.lit(process_date),"yyyy-MM-dd")
        ).withColumn(
                "end_dt",
                F.to_date(F.lit("3000-11-08"), "yyyy-MM-dd")
        ).withColumn(
                "rec_st",
                F.lit("1").cast(IntegerType())
        ).withColumn(
                "src_stm_id",
                F.lit("CUS"),
        ).withColumn(
                "ppn_dt",
                F.to_date(F.lit(process_date),"yyyy-MM-dd")
        ).select(
            "customer_id",
            "fn",
            "active",
            "club_member_status",
            "fashion_news_frequency",
            "eff_dt",
            "end_dt",
            "rec_st",
            "src_stm_id",
            "ppn_dt"
        )

writer = IcebergSCD2Writer(glue_context=glue_context, spark=spark, df=df_customers, df_target=df_target, job_output=job_output, process_date=process_date)
writer.write()



job.commit()