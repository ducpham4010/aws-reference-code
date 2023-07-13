from typing import List, Tuple, Optional
import boto3
from dataclasses import asdict, replace
from chalice.app import SQSEvent, CloudWatchEvent
from botocore.exceptions import ClientError
from .event.event_mapper import create_events
from .event.event_mapper import (
#     create_orcs_sqs_events,
    Event,
#     create_scheduled_events,
#     create_events,
#     create_s3_notify_sqs_events,
#     create_s3_cdc5_automated_notify_sqs_events,
)
from .event.params_parser import parse
from chalicelib.orchestration.const import EventType, EventMeta

from typing import List, Optional, Union


def _run_jobs(events: List[Event]) -> List[dict]:
    for event in events:
        event = asdict(event)
        name = event.get("name")
        params = {"--Params" : str(event.get("params"))}
    return run_glue_job(job_name = name, arguments = params)

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
    return _run_jobs(create_events(lambda_event))