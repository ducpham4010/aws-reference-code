import sys
import datetime
import boto3
import base64
from pyspark.sql import DataFrame, Row
from pyspark.context import SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, \
                            ['JOB_NAME', \
                            'aws_region', \
                            'output_path'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 sink locations
aws_region = args['aws_region']
output_path = args['output_path']

s3_target = output_path + "invoices_metrics"
checkpoint_location = output_path + "cp/"
#checkpoint_location = args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/"
temp_path = output_path + "temp/"


def processBatch(data_frame, batchId):
    now = datetime.datetime.now()
    year = now.year
    month = now.month
    day = now.day
    hour = now.hour
    minute = now.minute
    if (data_frame.count() > 0):
        dynamic_frame = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")
        apply_mapping = ApplyMapping.apply(frame = dynamic_frame, mappings = [ \
            ("data", "string", "data", "string"), \
            ("metadata", "string", "metadata", "string")],\
            transformation_ctx = "apply_mapping")
            
        data_frame = apply_mapping.toDF()
        data_frame.createOrReplaceTempView("invoices_temp")
        
        sql = """
        select json_tuple(r.json, 'invoice_id', 'customer_id', 'billing_date', 'due_date', 'balance', 'monthly_kwh_use', 'total_amount_due')
        from 
        (
            select 
                data as json
            from invoices_temp
        ) r
        """
        
        invoice_df = spark.sql(sql)
        invoice_dyf = DynamicFrame.fromDF(invoice_df, glueContext, "invoice_dyf")

        # Write to S3 Sink
        s3path = s3_target + "/ingest_year=" + "{:0>4}".format(str(year)) + "/ingest_month=" + "{:0>2}".format(str(month)) + "/ingest_day=" + "{:0>2}".format(str(day)) + "/ingest_hour=" + "{:0>2}".format(str(hour)) + "/"
 #      s3sink = glueContext.write_dynamic_frame.from_options(frame = apply_mapping, connection_type = "s3", connection_options = {"path": s3path}, format = "parquet", transformation_ctx = "s3sink")
        s3sink = glueContext.write_dynamic_frame.from_options(frame = invoice_dyf, connection_type = "s3", connection_options = {"path": s3path}, format = "parquet", transformation_ctx = "s3sink")

# Read from Kinesis Data Stream
#sourceData = glueContext.create_data_frame.from_catalog( \
#    database = "data-source", \
#    table_name = "invoices", \
#    transformation_ctx = "sourceData", \
#    additional_options = {"startingPosition": "TRIM_HORIZON", "inferSchema": "true"})
    
    
sourceData = glueContext.create_data_frame.from_options( \
   connection_type="kinesis", \
    connection_options={ \
        "typeOfData": "kinesis", \
        "streamARN": "arn:aws:kinesis:ap-southeast-1:039178755962:stream/kds_invoices", \
        "classification": "json", \
        "startingPosition": "LATEST", \
        "inferSchema": "true", \
    },
    transformation_ctx="sourceData",
)

sourceData.printSchema()

glueContext.forEachBatch(frame = sourceData, batch_function = processBatch, options = {"windowSize": "60 seconds", "checkpointLocation": checkpoint_location})

job.commit()