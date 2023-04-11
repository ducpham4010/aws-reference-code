import json
import boto3 
from datetime import datetime
from datetime import timedelta
import time
import os


def lambda_handler(event, context):
    client = boto3.client("redshift-data")
    clientGlue = boto3.client('glue')
    client_sm = boto3.client('secretsmanager')
    
    queryId = event["queryId"]
    etlDate = event["etlDate"]
    w4Flag = event["w4Flag"]
    
    secretNm        = os.environ.get('secretNm')
    secretArn       = os.environ.get('secretArn')
    # secretArn           = 'arn:aws:secretsmanager:ap-southeast-1:307750329693:secret:dev-dwh-redshift-secret-PE4jh0'
    print (1)
    
    response = client.get_statement_result(Id=queryId)
    
    print(response)

    responseSm = client_sm.get_secret_value(
        SecretId=secretNm
    )
   
    database_secrets = json.loads(responseSm['SecretString'])

    # print(database_secrets)
    redshift_database   = database_secrets['db_name']
    redshift_cluster_id = database_secrets['dbClusterIdentifier']
    
    TotalNumRows = response['TotalNumRows']
    print(TotalNumRows)
    strJobRunId = ""
    
    arguments = {
        '--class': 'GlueApp',
        '--pDate': etlDate,
        '--w4Flag' : w4Flag
    }
    
    print(arguments)
    
    for x in range(TotalNumRows):
        jobName = response['Records'][x-1][0]['stringValue']
        print(jobName)
        cur_dt7 = datetime.now() + timedelta(hours=7)
        print(cur_dt7)
        
        #insert log table status = 0 ( job running)
        sql = """
        insert into dwh.job_sts_log(status, start_tm, system_type, job_name, cob_dt, note)
        values (0, '{cur_dt7}', 'DWH','{JobName}' , to_date('{etlDate}', 'YYYY-MM-DD'), 'lambda start job');
        """.format(JobName=jobName, etlDate= etlDate, cur_dt7=cur_dt7) 
        
        print("Executing: {}".format(sql))

        responseExeSql = client.execute_statement(SecretArn = secretArn, Database=redshift_database, Sql=sql,ClusterIdentifier=redshift_cluster_id)
        time.sleep(5)
        print('start glue job')
        
        # Start glue job
        responseStart = clientGlue.start_job_run(JobName=jobName,Arguments=arguments)        
        # ,            Arguments=arguments
        
        strJobRunId = strJobRunId + "|" +responseStart['JobRunId']
        
    # return 1 
    return  strJobRunId