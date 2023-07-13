from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Optional, Tuple, List
from functools import reduce
import inspect

from ..const import DEFAULT_DATE_FORMAT, LEGACY_G2I_DATE_FORMAT
# from ..models.job_log import JobLog
# from chalicelib.env import get_env_vars

__registry = {}
__tags = defaultdict(set)


def parse(
    params: dict, event: Optional[dict] = None, config: Optional[dict] = None, condition_data: Optional[dict] = None
) -> dict:
    # only support first layer for now
    event = event or {}
    config = config or {}

    parsed_params = {}

    for key, val in params.items():
        if key.endswith("$"):
            parsed_params[key[:-1]] = call_func(val, event, config, condition_data)
        else:
            parsed_params[key] = val

    return parsed_params


def call_func(serialized_func: dict, event: dict, config: dict, condition_data: dict = None) -> Any:
    context = {"event": event, "config": config, "condition_data": condition_data}
    func, args, kwargs = _get_func(serialized_func)
    return func(context, *args, **kwargs)



def _get_func(serialized_func: dict):
    assert isinstance(serialized_func, dict)
    assert "func" in serialized_func

    func_name = serialized_func["func"]
    args = serialized_func.get("args", [])
    kwargs = serialized_func.get("kwargs", {})

    assert isinstance(args, list) and isinstance(kwargs, dict)
    assert func_name in __registry

    return __registry[func_name], args, kwargs
