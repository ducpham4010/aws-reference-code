import calendar
import datetime
import os
import re

import pyspark.sql.functions as F
from pyspark.sql.types import StringType


# from .utils import mask_word


@F.udf(returnType=StringType())
def get_value_of_key(first_col, k):
    """Find value of key = 1.1

    Args:
        first_col: A Dict
        key: key of first_col
    """
    if (not first_col) or (not isinstance(first_col, dict)):
        return None

    value = None
    for k1, val1 in first_col.items():
        if k1 == k:
            value = val1
            break

    return value


@F.udf(returnType=StringType())
def get_first_value_of_list(list_str):
    """Find first value in the list

    Args:
        list_str: A List
    """

    if (not list_str) or (not isinstance(list_str, list)):
        return None

    return list_str[0]


def get_first_day_of_next_month(input_date: datetime.date):
    if input_date.month == 12:
        year = input_date.year
        # fix day = 1 to avoid day = 31 and next month doesn't have 31th
        return input_date.replace(day=1, month=1, year=year + 1)

    month = input_date.month
    # fix day = 1 to avoid day = 31 and next month doesn't have 31th
    return input_date.replace(day=1, month=month + 1)


def get_last_day_of_next_month(input_date: datetime.date):
    if input_date.month == 12:
        year = input_date.year
        return input_date.replace(day=31, month=1, year=year + 1)

    next_month = input_date.month + 1
    year = input_date.year
    num_days_in_month = calendar.monthrange(year, next_month)[1]
    return input_date.replace(day=num_days_in_month, month=next_month)


@F.udf(returnType=StringType())
def validate_gso_email_address(email):
    email = str(email).lower()
    email = email.replace('-', '_')
    pattern = '^^\(([a-zA-Z0-9 _\-\.\+]+)\)\ ([a-zA-Z0-9 _\+\!\#\$\%\&\'\*\-\/\=\?\^\`\{\|\}\~\.]+)@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.)|(([a-zA-Z0-9\w _\!\#\$\%\&\'\*\-\/\=\?\^\`\{\|\}\~]+\.)+))([a-zA-Z]{2,24}|[0-9]{1,3})$|^([a-zA-Z0-9 \w _\+\!\#\$\%\&\'\*\-\/\=\?\^\`\{\|\}\~\.]+)@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.)|(([a-zA-Z0-9_\-]+\.)+))([a-zA-Z]{2,24}|[0-9]{1,3})'
    if re.search(pattern, str(email)):
        return (email)
    else:
        return ("")