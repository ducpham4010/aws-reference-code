import boto3
from botocore.exceptions import ClientError

session = boto3.session.Session()
client = session.client('glue')


# Define Lambda function
def get_sts_job(jobname, jobrunid): 
    jobName = jobname
    
    res = client.get_job_run( JobName=jobName,   RunId=jobrunid)
    
    print("******")
    print(res)
    
    status = res.get("JobRun").get("JobRunState")
    print("******")
    print(status)
    
    while status == "RUNNING":
        res = client.get_job_run( JobName=jobName,   RunId=jobrunid)
        status = res.get("JobRun").get("JobRunState")
        print(status)
    print("done")

get_sts_job('pre_action_rds_duph', 'jr_0ea92fcabc48305cf18ac03bef0a3018d53d1c9dff11182e353cecc4dd7e35c4')