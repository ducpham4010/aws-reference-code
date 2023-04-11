import json
import boto3 
import time
import os

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
    
response = client_sm.get_secret_value(SecretId=secretNm)
database_secrets = json.loads(response['SecretString'])
    
redshift_database   = database_secrets['dbname']
redshift_cluster_id = database_secrets['dbClusterIdentifier']
redshift_user = database_secrets['username']

print(redshift_user)

# sql = """	
#     Select status, cob_dt, job_name
#         From dwh.job_sts_log
#         Where cob_dt = to_date('{etlDate}', 'YYYY-MM-DD') 
#         And job_name = '{jobName}'	
# """.format(etlDate=etlDate, jobName = jobName, systemType=systemType)	

sql =  """ insert into dwh.test values ('123', '234') """

#responseExeSql = client.execute_statement(SecretArn = secretArn, Database=redshift_database, Sql=sql,ClusterIdentifier=redshift_cluster_id, DbUser=redshift_user)
responseExeSql = client.execute_statement(Database=redshift_database, Sql=sql,ClusterIdentifier=redshift_cluster_id, DbUser=redshift_user)
status = ""
while True:
        desc = client.describe_statement(Id=responseExeSql['Id'])
        status = desc["Status"]
        if status == "FINISHED":
            break
print(status)

#response = client.get_statement_result(Id=responseExeSql['Id'])

#sts = response['Records'][0][1]['stringValue']
print(responseExeSql)