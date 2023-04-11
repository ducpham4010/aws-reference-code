import json
import boto3
import time
import os

def lambda_handler(event, context):
    # TODO implement
    # initiate redshift-data client in boto3
    client = boto3.client("redshift-data")
    client_sm = boto3.client('secretsmanager')
    
    
    etlDate = event["etlDate"]
    grpObj  = event["grpObj"]
    secretNm        = os.environ.get('secretNm')
    secretArn       = os.environ.get('secretArn')
    
    # secretArn           = 'arn:aws:secretsmanager:ap-southeast-1:307750329693:secret:dev-dwh-redshift-secret-PE4jh0'
    
    response = client_sm.get_secret_value(
        SecretId=secretNm
    )
    database_secrets = json.loads(response['SecretString'])
    
    
    redshift_database   = database_secrets['db_name']
    redshift_cluster_id = database_secrets['dbClusterIdentifier']

    
    sql = """
        Select Count(1) from (
        select wt.job_name  
          from dwh.job_dependency wt
          left join dwh.job_sts_log lg1
               on wt.job_name = lg1.job_name
               and lg1.cob_dt = to_date('{etlDate}', 'YYYY-MM-DD') 
               and lg1.status = 1 --dieu kien job da hoan thanh
         where (1=1)
           AND wt.JOB_GRP  = '{grpObj}' 
         group by wt.job_name 
        having count(wt.job_name) <> count(lg1.job_name)  )
    """.format(etlDate=etlDate, grpObj = grpObj)
    
    responseExeSql = client.execute_statement(SecretArn = secretArn, Database=redshift_database, Sql=sql,ClusterIdentifier=redshift_cluster_id)
    
    desc = client.describe_statement(Id=responseExeSql['Id'])
    status = desc["Status"]
    
    # Check trạng thái chạy của query , chờ query chạy xong
    while status!="FINISHED":
        time.sleep(2)
        desc = client.describe_statement(Id=responseExeSql['Id'])
        status = desc["Status"]
 
    response = client.get_statement_result(Id=responseExeSql['Id']) ###'e3ce2789-7411-404f-8b59-dadb611bb411'    
    rst = response['Records'][0][0]['longValue']
    
    return rst