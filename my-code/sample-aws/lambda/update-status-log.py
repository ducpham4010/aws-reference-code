import json
import boto3
from datetime import datetime
from datetime import timedelta
import time
import os

def lambda_handler(event, context):
    # TODO implement
    client = boto3.client("redshift-data")
    client_sm = boto3.client('secretsmanager')
    
    #get input
    jobName     = event["jobName"]
    jobStatus   = event["jobStatus"]
    etlDate     = event["etlDate"]
    sysType     = event["systemType"] 	
    secretNm        = os.environ.get('secretNm')
    secretArn       = os.environ.get('secretArn')
    
    response = client_sm.get_secret_value(
        SecretId=secretNm
    )
    database_secrets = json.loads(response['SecretString'])
    
    # parameters for running execute_statement
    # secretArn           = 'arn:aws:secretsmanager:ap-southeast-1:307750329693:secret:dev-dwh-redshift-secret-PE4jh0'
    redshift_database   = database_secrets['db_name']
    redshift_cluster_id = database_secrets['dbClusterIdentifier']
    
    cur_dt7 = datetime.now() + timedelta(hours=7)
    
    sql = """
    begin transaction;
    
    delete from dwh.job_sts_log
    where job_name='{jobName}'
            and cob_dt = to_date('{etlDate}', 'YYYY-MM-DD')
            and system_type = '{sysType}';
            
    insert into dwh.job_sts_log (status, end_tm, system_type, job_name, cob_dt, note) 
    values ( {jobStatus}, '{cur_dt7}' , '{sysType}', '{jobName}', to_date('{etlDate}', 'YYYY-MM-DD'), 'lambda job done');

    end transaction;

    """.format(jobStatus = jobStatus , jobName=jobName, etlDate=etlDate, sysType=sysType, cur_dt7=cur_dt7)
    
    response = client.execute_statement(SecretArn = secretArn, Database=redshift_database, Sql=sql,ClusterIdentifier=redshift_cluster_id)
    time.sleep(3)
    
    rsp =  response['Id'] 
    if not rsp:  
        rsp = "0"
    else:
        rsp = "1"
    return rsp