import boto3
import pandas as pd

#jobName         = event["jobName"]
jobName         = "TWT_IP_2_IP_DUCPH" 
#systemType      = event["systemType"]
systemType      = "NULL"
#etlDate         = event["etlDate"]
etlDate         = "2022-09-30"
#secretNm        = os.environ.get('secretNm')
secretNm        = "secret-redshift-ducph"
#secretArn       = os.environ.get('secretArn')
secretArn           = 'arn:aws:secretsmanager:ap-southeast-1:039178755962:secret:awm-db-key-qKWLmp'
sql = """	
    Select status, job_name
        From dwh.job_sts_log
        Where cob_dt = to_date('{etlDate}', 'YYYY-MM-DD')
        And job_name = '{jobName}'	
""".format(etlDate=etlDate, jobName = jobName, systemType=systemType)

client = boto3.client('redshift-data')

def post_process(meta, records):
    columns = [k["name"] for k in meta]
    rows = []
    for r in records:
        tmp = []
        for c in r:
            tmp.append(c[list(c.keys())[0]])
        rows.append(tmp)
    return pd.DataFrame(rows, columns=columns)

def query(sql, cluster="redshift-cluster-1", user="awsuser", database="dev"):
    resp = client.execute_statement(
        Database=database,
        ClusterIdentifier=cluster,
        DbUser=user,
        Sql=sql
    )
    qid = resp["Id"]
    print(qid)
    desc = None
    while True:
        desc = client.describe_statement(Id=qid)
        if desc["Status"] == "FINISHED":
            break
    if desc and desc["ResultRows"]  > 0:
        result = client.get_statement_result(Id=qid)
        rows, meta = result["Records"], result["ColumnMetadata"]
        #print(rows)
        return post_process(meta, rows)

pf=query(sql)
print(pf)