import json
import time
from enum import unique, Enum
from dataclasses import dataclass, field, asdict
from json import JSONDecodeError
from typing import List, Optional, Union
# from urllib.parse import unquote
# from chalicelib.env import get_env_vars
# from ..services.sqs_utils import SQSMessage


# from chalice.app import SQSEvent, SQSRecord, CloudWatchEvent
from marshmallow import Schema, fields, validate, post_load, ValidationError, EXCLUDE

from chalicelib.orchestration.const import EventType, EventMeta
#from chalicelib.utils.logging import get_logger
from .params_parser import parse
from ..exceptions import BadArgumentException

#logger = get_logger(__name__)


@dataclass(frozen=True)
class Event:
    name: str
    params: dict
    event_type: str
    meta: dict = field(default_factory=dict)

    def should_delay(self):
        return EventMeta.PROCESS_TIMESTAMP in self.meta and self.meta[EventMeta.PROCESS_TIMESTAMP] > int(time.time())

    def as_dict(self):
        return json.loads(json.dumps(asdict(self)))


class EventSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    name = fields.String(required=True)
    params = fields.Dict(required=False, load_default=dict)
    meta = fields.Dict(required=False, load_default=dict)
    event_type = fields.String(
        required=False, validate=validate.OneOf(EventType.values()), load_default=EventType.ORCS_EVENT
    )

    @post_load
    def parse_event_params(self, data, **kwargs):
        data["params"] = parse(data["params"])
        return data


def create_events(lambda_event: Union[dict, List[dict]]) -> List[Event]:
    if isinstance(lambda_event, dict):
        return [Event(**EventSchema().load(lambda_event))]
    elif isinstance(lambda_event, list):
        return [Event(**EventSchema().load(e)) for e in lambda_event]
    else:
        raise BadArgumentException("event data type not supported")
    
# ls =[{
#         "name": "ducph_glue_job_pt_test",
#         "event_type": "MANUAL",
#         "params": {
#             "data_date" : "2023-01-01",
#             "skip_conditions" : True 
#         }
#     },
#     {

#         "name": "ducph_glue_job_pt_test2",
#         "event_type": "MANUAL",
#         "params": {
#             "data_date" : "2023-01-01",
#             "skip_conditions" : True 
#         }
#     }]


# event = [Event(**EventSchema().load(e)) for e in ls]
# print(event)

# =>>
# [
#   Event(name='ducph_glue_job_pt_test', params={'data_date': '2023-01-01', 'skip_conditions': True}, event_type='MANUAL', meta={}), 
#   Event(name='ducph_glue_job_pt_test2', params={'data_date': '2023-01-01', 'skip_conditions': True}, event_type='MANUAL', meta={})
# ]