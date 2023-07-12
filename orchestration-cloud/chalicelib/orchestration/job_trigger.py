from typing import List, Tuple, Optional
import boto3
from chalice.app import SQSEvent, CloudWatchEvent
from botocore.exceptions import ClientError

from typing import List, Optional, Union


def _run_jobs(events):
    job_name = events["job_name"]
    arguments = events["arguments"]
    return run_glue_job(job_name=job_name, arguments=arguments)

def run_glue_job(job_name, arguments = {}):
    session = boto3.session.Session()
    glue_client = session.client('glue')
    try:
        job_run_id = glue_client.start_job_run(JobName=job_name, Arguments=arguments)
        return job_run_id.get("JobRunId")
    except ClientError as e:
        raise Exception( "boto3 client error in run_glue_job_get_status: " + e.__str__())
    except Exception as e:
        raise Exception( "Unexpected error in run_glue_job_get_status: " + e.__str__())



def trigger_single_event_jobs(lambda_event):
    return _run_jobs(lambda_event)