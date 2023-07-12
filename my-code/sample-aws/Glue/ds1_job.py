from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from awsglue.job import Job
from typing import Type, List
import sys
import json
import boto3
from awsglue.context import GlueContext
import uuid


@F.udf
def create_uuid(some_string):
    return str(uuid.uuid5(uuid.NAMESPACE_OID, some_string))


"""
set up glue job :
--additional-python-modules : s3://tcb-ps5-data-uat1b-artifacts-ap-southeast-1-481362995327/pip_pkg/propagation_curated/jeda-1.1.0-py3-none-any.whl,s3://tcb-ps5-data-uat1b-artifacts-ap-southeast-1-481362995327/pip_pkg/propagation_curated/marshmallow-3.13.0-py2.py3-none-any.whl,s3://tcb-ps5-data-uat1b-artifacts-ap-southeast-1-481362995327/pip_pkg/propagation_curated/quinn-0.9.0-py2.py3-none-any.whl
--conf : spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://tcb-ps5-data-uat1b-golden-ap-southeast-1-481362995327/golden_curated --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.parquet.enableVectorizedReader=true --conf spark.sql.catalog.glue_catalog.cache-enabled=false
--continuous-log-logGroup : ds1-uat1b-apse-1-glue_log_group
--datalake-formats : iceberg
--enable-continuous-log-filter : true

IAM Role : ds1-uat1b-apse-1-glue-jb-iam-role
"""

def get_secret(secret_name="dev.es.credential"):
    region_name = "ap-southeast-1"
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    if "SecretString" in get_secret_value_response:
        secret = get_secret_value_response["SecretString"]
        sc = json.loads(secret)
        print("secret :", sc)
        return sc
    else:
        print("not support")


def pgjdbc_read(glue_context, secret_name, table_name):
    pg_secrets = get_secret(secret_name)
    pg_uri = (
        f"jdbc:postgresql://{pg_secrets['read_only_endpoint']}:{pg_secrets['port']}/{pg_secrets['database_name']}"
    )
    pg_options = {
        "url": pg_uri,
        "user": pg_secrets["username"],
        "password": pg_secrets["password"],
        "dbtable": table_name
    }
    dynamic_frame = glue_context.create_dynamic_frame.from_options(
        connection_type="postgresql", connection_options=pg_options
    )
    df = dynamic_frame.toDF()
    print("Read DF from JDBC with schema:\n")
    print(df.dtypes)
    yield df

def delete_iceberg_tbl(glue_context, catalog_name, db_name, tbl_name):
    query = """DELETE FROM {0}.`{1}`.{2}""".format(catalog_name,db_name,tbl_name)
    try:
        glue_context.spark_session.sql(query)
    except Exception:
        raise Exception("Delete is not successful")

def run_query(glue_context, query):
    try:
        df = glue_context.spark_session.sql(query)
        return df
    except Exception:
        raise Exception("Query is not supported")
        
def gen_record_by_key(glue_context, db_name, table_name, columns_key):
    dyf = glue_context.create_dynamic_frame.from_catalog(database=db_name, table_name=table_name)
    df = dyf.toDF()
    df = df.withColumn(columns_key, create_uuid(F.col(columns_key).cast(StringType())))
    df.createOrReplaceTempView("gen_df")
    query_insert_new_data = """
        INSERT INTO {0}.{1}
        SELECT * FROM gen_df
    """.format(db_name,table_name)
    print(query_insert_new_data)
    glue_context.spark_session.sql(query_insert_new_data)
    return df

    


arguments = sys.argv
args = getResolvedOptions(arguments, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
job = Job(glue_context)
job.init(args["JOB_NAME"], args)




#################################################################################################################################
# secret_name = "tcb-ps5-data-uat1b-aurora-golden"
# tbl_name = "ms1_curated.tbl_cv_ref_product_tree"
# df = next(pgjdbc_read(glue_context, secret_name=secret_name, tbl_name=table_name))
# df.show(10)
# df.write.format("parquet").option("quote", None).mode("overwrite").save("s3://tcb-ps5-data-uat1b-golden-ap-southeast-1-481362995327/golden_ods/tbl_cv_ref_product_tree/")


#################################################################################################################################
# catalog_name = "glue_catalog"
# db_name = "tcb-uat1b-golden_curated"
# tbl_name = "lending_product_holding"

# delete_iceberg_tbl(glue_context, catalog_name=catalog_name, db_name=db_name, tbl_name=tbl_name)


################################################################################################################################
# query = """
# CREATE EXTERNAL TABLE `tcb-uat1b-golden_curated`.ms1_stg_cv_product_input_t24_test(
#   customer_id bigint, 
#   arr_id string, 
#   tnr_tp_id string, 
#   ivtor_id string, 
#   adj_ar_bsn_line string, 
#   prgm_cl_id string, 
#   polcy_cl_id string, 
#   adj_ou_id string, 
#   pd_id string, 
#   sector_id string, 
#   imt_tp_id string, 
#   group_acc_tcb_id string, 
#   lmt_sec_tp_id string, 
#   acct_grp_id string, 
#   lc_type_id string, 
#   amount_ind decimal(38,6), 
#   pd_cgy_code_cl_id string, 
#   loan_pps_cl_id string, 
#   gl_tnr_tp_id string, 
#   cls_bal_fcy decimal(38,6), 
#   cls_bal_lcy decimal(38,6), 
#   ccy_code string, 
#   info_detail array<map<string,string>>, 
#   ds_source string, 
#   ds_time_id bigint, 
#   ds_load_time timestamp)
# ROW FORMAT SERDE 
#   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
# STORED AS INPUTFORMAT 
#   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
# OUTPUTFORMAT 
#   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
# LOCATION
#   's3://tcb-ps5-data-uat1b-golden-ap-southeast-1-481362995327/golden_curated/ms1_stg_cv_product_input_t24_test/'
# TBLPROPERTIES (
#   'classification'='parquet')
# """
# df = run_query(glue_context, query)
# df.show(10)


################################################################################
db_name = "sampledb"
table_name = "transaction_pt"
columns_key = "cust_id"
df = gen_record_by_key(glue_context, db_name, table_name, columns_key)
df.show(10)


job.commit()