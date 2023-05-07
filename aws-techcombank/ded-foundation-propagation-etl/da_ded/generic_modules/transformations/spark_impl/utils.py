import re
import string
from re import sub

from dateutil.parser import parse
from pyspark.sql import column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import coalesce, to_date, date_format


def get_columns_from_column_patterns(df, column_patterns: list):
    column_set = set()
    for pattern in column_patterns:
        for col in df.columns:
            if re.compile(pattern).fullmatch(col):
                column_set.add(col)
    return column_set


def get_col_type(df: DataFrame, col: str):
    return df.select(col).schema[0].dataType.typeName()


def spark_convert_date_to_yyyymmdd(col: column, format: str = None):
    """`col` must be a Spark column of StringType()
    if `format` is not provided, try common formats and return the first one which parse correctly (much more expensive)
    """
    if not format:
        formats = (
            None,
            "dd/MM/yyyy",
            "MM/dd/yyyy",
            "yyyy/MM/dd",
            "dd-MM-yyyy",
            "MM-dd-yyyy",
            "yyyy-MM-dd",
            "dd/MM/yy",
            "MM/dd/yy",
            "dd-MM-yy",
            "MM-dd-yy",
            "yyyyMMdd",
            "ddMMyyyy",
            "yyMMdd",
            "ddMMyy",
        )
        return coalesce(*[date_format(to_date(col, f), "yyyyMMdd") for f in formats])
    return date_format(to_date(col, format), "yyyyMMdd")


def spark_convert_date_to_ds_partition_date_format(col: column, format: str = None):
    """`col` must be a Spark column of StringType()
    if `format` is not provided, try common formats and return the first one which parse correctly (much more expensive)
    """
    if not format:
        formats = (
            None,
            "dd/MM/yyyy",
            "MM/dd/yyyy",
            "yyyy/MM/dd",
            "dd-MM-yyyy",
            "MM-dd-yyyy",
            "yyyy-MM-dd",
            "dd/MM/yy",
            "MM/dd/yy",
            "dd-MM-yy",
            "MM-dd-yy",
            "yyyyMMdd",
            "ddMMyyyy",
            "yyMMdd",
            "ddMMyy",
        )
        return coalesce(*[date_format(to_date(col, f), "yyyy-MM-dd") for f in formats])
    return date_format(to_date(col, format), "yyyy-MM-dd")


def to_snake_case(s):
    return "_".join(sub("([A-Z][a-z]+)", r" \1", sub("([A-Z]+)", r" \1", s.replace("-", " "))).split()).lower()


def get_year_month(string):
    try:
        parsed_date = parse(string, fuzzy=False)
        year = str(parsed_date.year)
        month = str(parsed_date.month)
        day = str(parsed_date.day)
        return [year, month, day]

    except:
        return None


def normalize_phone(phone_number: str):
    return phone_number.strip()