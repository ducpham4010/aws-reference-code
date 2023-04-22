from pyspark.sql.session import SparkSession
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark import SparkContext
from awsglue.job import Job
import sys
import json

# input = {
#     "Inputs": {
#         "glue_catalog": {
#             "database" : "database_name",
#             "table" : "table_name",
#             "partition_column" : "",
#             "transformation_ctx" : [],
#             "additional_options" : {}
#         },
#         "reader_type" : "catalog",
#         "query": ""
#     },
#     "Outputs": "",
#     "Params": {"data_date": "2022-01-01"}
# }

class CatalogReader():
    def __init__(self, job_input: dict, spark: SparkSession, glue_context: GlueContext, job_config=None):
        self.job_config = job_config
        self.job_input = job_input
        self.glue_context = glue_context
        self.spark = spark
    
    def read(self, predicate_value=None):
        reader_type = self.job_input.get("reader_type")
        if reader_type == 'catalog':
            try:
                database = self.job_input.get("glue_catalog").get("database")
                table = self.job_input.get("glue_catalog").get("table")
                partition_column = self.job_input.get("glue_catalog").get("partition_column")
                push_down_predicate = f"{partition_column}=='{predicate_value}'"
                transformation_ctx = 'transformation_ctx'
                additional_options = {}
                
                df = self.glue_context.create_dynamic_frame.from_catalog(
                    database = database,
                    table_name = table,
                    transformation_ctx = transformation_ctx,
                    push_down_predicate = push_down_predicate,
                    additional_options = additional_options
                ).toDF()
                return df
            except Exception as exp:
                print(exp)
                raise exp
        elif reader_type == 'catalog_sql':
            try:
                df = self.spark.sql(self.job_input.query)
                return df
            except Exception as exp:
                print(exp)
                raise exp
        else :
            print(f"Unsupport reader type {reader_type} now")

arguments = sys.argv
args = getResolvedOptions(arguments, ['JOB_NAME', 'Inputs', 'Outputs', 'Params'])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

input_data_str = args.get('Inputs')
job_input = json.loads(input_data_str)

params_str = args.get("Params")
params = json.loads(params_str)

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

reader = CatalogReader(job_input, spark, glue_context, None)
df = reader.read()
df.show()

job.commit()
