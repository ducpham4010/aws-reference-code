import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SQLContext
from awsglue.job import Job
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame
import pandas as pd
import boto3
import json
from pyspark.sql.functions import lit
from boto3.dynamodb.conditions import Key, Attr
import calendar
import boto3
import json
from datetime import datetime,date, timedelta
import pytz

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#pDate = str(date.today())
pDate = '2022-09-30'
pTime = str(datetime.now())
#pTime = str(datetime.now().strftime("%H:%M:%S"))

def getCredentials(secretname):
    credential = {}
    
    secret_name = secretname
    region_name = "ap-southeast-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    get_secret_value_response = client.get_secret_value(
      SecretId=secret_name
    )
    
    secret = json.loads(get_secret_value_response['SecretString'])
    
    print(secret)
    
    credential['username'] = secret['username']
    credential['password'] = secret['password']
    credential['port'] = secret['port']
    credential['host'] = secret['host']
    
    if "url" in secret:
        credential['url'] = secret['url']
    else:
        credential['url'] = ""
        
    #Dwh
    if "s3bucket" in secret:
        credential['s3bucket'] = secret['s3bucket']
    
    if "catalogdb" in secret:
        credential['catalogdb'] = secret['catalogdb']
        
    if "dbInstanceIdentifier" in secret:
        credential['catalogdb'] = secret['dbInstanceIdentifier']
        
    if "db_name" in secret:
        credential['dbname'] = secret['db_name']
        
    if "dbname" in secret:
        credential['dbname'] = secret['dbname']
    return credential
    
    
credential = getCredentials("secret-redshift-ducph") 
print(credential)
url = credential['url']
us = credential['username']
ps = credential['password']
dbname = credential['dbname']
s3bucket = credential['s3bucket']

host = credential['host']
port = credential['port']

TWT_IP_DyF = glueContext.create_dynamic_frame_from_options(connection_type = 'redshift', 
    connection_options = {"url": "jdbc:redshift://" + host + ":" + str(port) +"/"+ dbname, "user": us, "password": ps, "dbtable": "dwh.TWT_IP", "redshiftTmpDir": args["TempDir"]}, transformation_ctx = "TWT_IP" )
#jdbc:redshift://redshift-cluster-1.caysi7ehlfkv.ap-southeast-1.redshift.amazonaws.com:5439/dev

sql_Table_pd_dim = """(select * from {DWH}.TWT_IP where unq_id_in_src_stm = 14525) as t"""
PD_DIM_DF = glueContext.read.format("jdbc").option("url","jdbc:redshift://" + host + ":" + str(port) +"/"+ dbname).option("user",us).option("password",ps).option("driver", "com.amazon.redshift.jdbc.Driver").option("dbtable",sql_Table_pd_dim).load()
PD_DIM_DF.show()

