import json
import boto3 
import time
import os

# from datetime import datetime
# from datetime import datetime,date, timedelta

def lambda_handler(event, context):
    # TODO implement
    client = boto3.client("redshift-data")
    
    etlDate = event["etlDate"]
    grpObj  = event["grpObj"]
    # secretNm = event["secretNm"]
    #secretArn = event["secretNm"]
    secretNm        = os.environ.get('secretNm')
    secretArn       = os.environ.get('secretArn')
	# secretArn           = 'arn:aws:secretsmanager:ap-southeast-1:307750329693:secret:dev-dwh-redshift-secret-PE4jh0'
    
    # status, upd_tm, src_stm_cd, job_name, cob_dt, note
    
    sql = """	
        select wt.job_name	
          from dwh.job_dependency wt           	
          left join dwh.job_sts_log lg1   	
            ON 1=1
           and SPLIT_PART(wt.job_dependency, '.', 1 )  = lg1.system_type	
           and SPLIT_PART(wt.job_dependency , '.', 2 ) = lg1.job_name 	
           and lg1.cob_dt = to_date('{etlDate}', 'YYYY-MM-DD')	
           and lg1.status = 1
         where (1=1) 	
           and wt.JOB_GRP = '{grpObj}'	
           and not exists (select 1 from dwh.job_sts_log lg2 	
                            where 1=1	
                              and lg2.cob_dt  = to_date('{etlDate}', 'YYYY-MM-DD')	
                              and wt.job_name =lg2.job_name) 	
         group by wt.job_name	
         having count(wt.job_name)=count(lg1.job_name) 	
    """.format(etlDate=etlDate, grpObj = grpObj)	
    print(sql)

    client_sm = boto3.client('secretsmanager')

    response = client_sm.get_secret_value(
        SecretId=secretNm
    )
    print(sql)
    
    # pCurrentTimeStamp = from_utc_timestamp(current_timestamp(),'Asia/Ho_Chi_Minh')
    # print(pCurrentTimeStamp)

    database_secrets = json.loads(response['SecretString'])

    print(database_secrets)

    # parameters for running execute_statement    
    redshift_database   = database_secrets['db_name']
    sql_text            = sql
    redshift_cluster_id = database_secrets['dbClusterIdentifier']
   
        
    print("Executing: {}".format(sql_text))
    
    response = client.execute_statement(SecretArn = secretArn, Database=redshift_database, Sql=sql_text,ClusterIdentifier=redshift_cluster_id)
    time.sleep(2)
    desc = client.describe_statement(Id=response['Id'])
    status = desc["Status"]
    
    while status!="FINISHED":
        time.sleep(2)
        desc = client.describe_statement(Id=response['Id'])
        status = desc["Status"]
        
    return response['Id'] 