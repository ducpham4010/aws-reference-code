from datetime import datetime
import pytz
import re

from marshmallow import Schema, fields, validate
from jeda.models.job_config import JobConfigSchema
from jeda.models.job_output import JobOutput
from .job_input import CuratedJobInput

class CuratedJobConfig:
    def __init__(self, job_config: dict):
        job_config = JobConfigSchema().load(job_config)
        self.input = [CuratedJobInput(v) for v in job_config['input']]
        self.output = [JobOutput(v) for v in job_config['output']]
        self.execution_engine = job_config['execution_engine']
        self.job_config_version = job_config['job_config_version']
        self.data_date_offset = job_config['data_date_offset']
        self.cob_date = job_config["params"].get("event", {}).get("cob_date")