import pyspark.sql.functions as F
from pyspark.sql.types import DateType

from da_ded.exceptions import BaseException
from da_ded.generic_modules.models.base_models import DotDict
from da_ded.generic_modules.models.constants import DsColumn
from da_ded.normalizations import (
    normalize_and_uppercase_vietnamese_name,
    remove_redundant_spaces,
)
from .constants import SPARK_TIMEZONE_CFG
from .utils import get_columns_from_column_patterns, get_col_type


def _text_non_unicode_udf(value):
    if type(value) is str:
        return normalize_and_uppercase_vietnamese_name(remove_redundant_spaces(value))
    if type(value) is list:
        for idx, v in enumerate(value):
            value[idx] = _text_non_unicode_udf(value)
        return value
    if type(value) is dict:
        for k, v in value.items():
            value[k] = _text_non_unicode_udf(v)
        return value
    return None


def da_text_non_unicode(df, trf):
    trf = DotDict(trf)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        return_type = df.select(col).schema[0].dataType
        spark_udf = F.udf(_text_non_unicode_udf, returnType=return_type)
        df = df.withColumn(col, spark_udf(df[col]))
    return df


def _text_with_unicode_udf(value):
    if type(value) is str:
        return remove_redundant_spaces(value).upper()
    if type(value) is list:
        for idx, v in enumerate(value):
            value[idx] = _text_with_unicode_udf(value)
        return value
    if type(value) is dict:
        for k, v in value.items():
            value[k] = _text_with_unicode_udf(v)
        return value
    return None


def da_text_with_unicode(df, trf):
    trf = DotDict(trf)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        return_type = df.select(col).schema[0].dataType
        spark_udf = F.udf(_text_with_unicode_udf, returnType=return_type)
        df = df.withColumn(col, spark_udf(df[col]))
    return df


def da_date(df, trf):
    trf = DotDict(trf)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        if get_col_type(df, col) == "timestamp":
            df = df.withColumn(col, F.to_date(df[col], format=trf.date_format).alias(col))
        elif get_col_type(df, col) == "string":
            df = df.withColumn(col, df[col].cast(DateType()))
        else:
            raise BaseException(f"da_date: field {col} must have timestamp type or string type")
    return df


def da_date_time(df, trf):
    trf = DotDict(trf)
    backup_tz_conf = trf.spark_env.spark.conf.get(SPARK_TIMEZONE_CFG)
    trf.spark_env.spark.conf.set(SPARK_TIMEZONE_CFG, trf.timezone)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        df = df.withColumn(col, F.to_timestamp(df[col], format=trf.timestamp_format).alias(col))
    trf.spark_env.spark.conf.set(SPARK_TIMEZONE_CFG, backup_tz_conf)
    return df


def da_get_snapshot_from_scd_type_2(df, trf):
    df = df.drop(DsColumn.RECORD_UPDATED_TIMESTAMP)
    df = df.drop(DsColumn.RECORD_STATUS)
    df = df.drop(DsColumn.RECORD_CHANGE_TYPE)
    df = df.drop(DsColumn.PARTITION_DATE)
    df = df.drop(DsColumn.CDC_INDEX)
    df = df.withColumn(DsColumn.PROCESS_DATE, F.date_sub(F.current_date(), 1))
    return df