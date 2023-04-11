import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3
import json
from boto3.dynamodb.conditions import Key, Attr
import redshift_connector

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# dynamodb = boto3.resource("dynamodb")
# table = dynamodb.Table("param")
# resp = table.scan(AttributesToGet = ['pdate'])
# pdate = resp['Items'][0]['pdate']

def getCredentials(secretname):
    credential = {}
    
    secret_name = secretname
    region_name = "ap-southeast-1"
    
    session = boto3.session.Session()
    client = session.client(
        service_name = 'secretsmanager',
        region_name = region_name
    )
    
    get_secret_value_response = client.get_secret_value(
        SecretId = secretname
        )
        
    secret = json.loads(get_secret_value_response['SecretString'])
    
    print(secret)

    credential['username'] = secret['username']
    credential['password'] = secret['password']
    credential['port'] = secret['port']
    credential['host'] = secret['host']
    credential['dbname'] = secret['dbname']
    credential['dbClusterIdentifier'] = secret['dbClusterIdentifier']
    
    return credential

credential = getCredentials("luc-luu-redshift")

print(credential)
us = credential['username']
ps = credential['password']
host = credential['host']
port = credential['port']
dbname = credential['dbname']
dbClusterIdentifier = credential['dbClusterIdentifier']

with redshift_connector.connect(
    host= host,
    database= dbname,
    user= us,
    password= ps,
    ssl = False
    # port value of 5439 is specified by default
) as conn:
    with conn.cursor() as cursor:
      # Please note: autocommit is disabled by default, per DB-API specification
      # If you'd like to commit your changes, manually commit or enable autocommit
      # on the cursor object
      # conn.commit()  # manually commits
      # conn.autocommit = True  # enables autocommit for subsequent SQL statements

        cursor.execute("""
        INSERT INTO "stag"."twt_rsc_dim_vd" (rsc_id, rsc_code, eff_dt, end_dt)
SELECT ri_id, unq_id_in_src_stm, eff_dt, end_dt
FROM "stag"."ri_vd"
WHERE ppn_dt = 20221215
        """)
        conn.commit()
        conn.close


job.commit()