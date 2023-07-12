import os

os.environ["PYNAMODB_CONFIG"] = os.path.abspath(os.path.join(os.path.dirname(__file__), "pynamodb_settings.py"))

from chalice import Blueprint
from chalice.app import SQSEvent, Rate, CloudWatchEvent

from .job_trigger import (
    trigger_single_event_jobs
)

orcs_handlers = Blueprint(__name__)

@orcs_handlers.lambda_function()
def handle_single_event(event, context):
    trigger_single_event_jobs(event)



























