import boto3
import pandas as pd

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

pf=query("select * from dwh.ip where unq_id_in_src_stm = 001")
print(pf)