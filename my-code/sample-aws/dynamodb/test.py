import os
import time

# Import Boto 3 for AWS Glue
import boto3
from botocore.exceptions import ClientError
client = boto3.client('glue')


# Define Lambda function
def run_job(jobname): 
    jobName = jobname
    
    rspStart = client.start_job_run(JobName = jobName)
    res = client.get_job_run( JobName=jobName,   RunId=rspStart["JobRunId"])
    
    print("******")
    print(res)
    
    status = res.get("JobRun").get("JobRunState")
    print("******")
    print(status)
    
    while status!="SUCCEEDED":
        #print("******")
        #time.sleep(10)
        status = res.get("JobRun").get("JobRunState")
        print(status)
    
    return rspStart["JobRunId"]


def run_glue_job_get_status(job_name, arguments = {}):
   session = boto3.session.Session()
   glue_client = session.client('glue')
   try:
      job_run_id = glue_client.start_job_run(JobName=job_name, Arguments=arguments)
      status_detail = glue_client.get_job_run(JobName=job_name, RunId = job_run_id.get("JobRunId"))
      status = status_detail.get("JobRun").get("JobRunState")
      return status
   except ClientError as e:
      raise Exception( "boto3 client error in run_glue_job_get_status: " + e.__str__())
   except Exception as e:
      raise Exception( "Unexpected error in run_glue_job_get_status: " + e.__str__())


def run_glue_job(job_name, arguments = {}):
    session = boto3.session.Session()
    glue_client = session.client('glue')
    try:
        job_run_id = glue_client.start_job_run(JobName=job_name, Arguments=arguments)
    except ClientError as e:
        raise Exception( "boto3 client error in run_glue_job_get_status: " + e.__str__())
    except Exception as e:
        raise Exception( "Unexpected error in run_glue_job_get_status: " + e.__str__())

id = 'jr_0826deeffb2715958ee5c224e1a465664c4a9f577d73b5a88b3e5f945f9d73e2'

def get_status_glue_job(job_name):
    session = boto3.session.Session()
    glue_client = session.client('glue')
    try:
        status_detail = glue_client.get_job_run(JobName=job_name, RunId = id)
        status = status_detail.get("JobRun").get("JobRunState")
        return status
    except ClientError as e:
        raise Exception( "boto3 client error in run_glue_job_get_status: " + e.__str__())
    except Exception as e:
        raise Exception( "Unexpected error in run_glue_job_get_status: " + e.__str__())



#run_glue_job('TWT_IP_2_IP_DUCPH')

print(get_status_glue_job('TWT_IP_2_IP_DUCPH'))


