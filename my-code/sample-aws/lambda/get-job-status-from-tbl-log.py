import json
import boto3 
import time
import os


def lambda_handler(event, context):
    # TODO implement
    client = boto3.client("redshift-data")
    client_sm = boto3.client('secretsmanager')
    
    #jobName         = event["jobName"]
    jobName         = "TWT_IP_2_IP_DUCPH" 
    #systemType      = event["systemType"]
    systemType      = ""
    #etlDate         = event["etlDate"]
    etlDate         = "2022-09-30"
    #secretNm        = os.environ.get('secretNm')
    secretNm        = "secret-redshift-ducph"
    #secretArn       = os.environ.get('secretArn')
    secretArn           = 'arn:aws:secretsmanager:ap-southeast-1:039178755962:secret:awm-db-key-qKWLmp'
    
    # print(secretNm)
    # print(secretArn)
    
    response = client_sm.get_secret_value(
        SecretId=secretNm
    )
    database_secrets = json.loads(response['SecretString'])
    
    redshift_database   = database_secrets['dbname']
    redshift_cluster_id = database_secrets['dbClusterIdentifier']
    redshift_user = database_secrets['username']

    # print(database_secrets)

    sql = """	
        Select status
            From dwh.job_sts_log
            Where cob_dt = to_date('{etlDate}', 'YYYY-MM-DD') 
            And job_name = '{jobName}'	
    """.format(etlDate=etlDate, jobName = jobName, systemType=systemType)	

    #responseExeSql = client.execute_statement(SecretArn = secretArn, Database=redshift_database, Sql=sql,ClusterIdentifier=redshift_cluster_id)
    responseExeSql = client.execute_statement(Database=redshift_database, Sql=sql,ClusterIdentifier=redshift_cluster_id, DbUser=redshift_user)

    #check ket qua thuc thi sql
    status = ""
    while True:
        desc = client.describe_statement(Id=responseExeSql['Id'])
        status = desc["Status"]
        if status == "FINISHED":
            break
    print(status)
        
    response = client.get_statement_result(Id=responseExeSql['Id'])   

    if response['TotalNumRows'] > 0: 
        sts = response['Records'][0][0]['longValue']
        # print(sts)
        if sts == 1:
            # print("job done")
            return "COMPLETED"
        else:
            # print("job running") 
            return "FAIL"
    else: #chua ton tai trong bang log
        return "FAIL"