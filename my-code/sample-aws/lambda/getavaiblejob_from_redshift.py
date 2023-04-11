import json
import boto3


def lambda_handler(event, context):
    # TODO implement
    # initiate redshift-data client in boto3
    client = boto3.client("redshift-data")
    
    
    etlDate = event["etlDate"]
    grpObj  = event["grpObj"]
    
    sql = """
        select wt.object_name--, count(wt.object_name),count(lg1.object)
          from tbl_waiting wt           
          left join etl_log lg1   
           ON SPLIT_PART(wt.WAITING, '.', 1 ) = lg1.system_type
           and SPLIT_PART(wt.WAITING, '.', 2 ) = lg1.object 
           and lg1.process_date = to_date('{etlDate}', 'YYYYMMDD')
           and lg1.flag = 1
         where (1=1) 
           and wt.GRP_OBJ = '{grpObj}'
           and wt.status = 'Y'           
           and not exists (select 1 from etl_log lg2 
                            where lg2.SYSTEM_TYPE='DWH'
                              and lg2.process_date = to_date('{etlDate}', 'YYYYMMDD')
                              and wt.OBJECT_NAME=lg2.object) 
         group by wt.object_name
         having count(wt.object_name)=count(lg1.object) 
    """.format(etlDate=etlDate, grpObj = grpObj)
    
    # parameters for running execute_statement
    secretArn           = 'arn:aws:secretsmanager:ap-southeast-1:039178755962:secret:awm-db-key-qKWLmp'
    redshift_database   = 'awm'
    redshift_user       = 'sorus'
    sql_text            = sql
    redshift_cluster_id = 'redshift-cluster-awm'
        
    print("Executing: {}".format(sql_text))
    
    response = client.execute_statement(SecretArn = secretArn, Database=redshift_database, Sql=sql_text,ClusterIdentifier=redshift_cluster_id)
    
    # response1 = client.get_statement_result(
    #     Id=response['Id']
    # )
    
    
    # response['Id'] 
    if len(response['Id']) == 0:
        return "-1"
    else:
        return response['Id'] 