import datetime
import inspect
from typing import List, Dict

from pyspark.sql import DataFrame, Window

from da_ded.exceptions import BaseException
from . import transformations_schema
from ..models.constants import DsColumn
import pyspark.sql.functions as F


def validate_and_load_transformations(transformations: list) -> List[Dict]:
    for idx, trf in enumerate(transformations):
        name = trf.get("name", None)
        if not name:
            raise BaseException(f"Transformation {trf} does not have `name`")
        schema = getattr(transformations_schema, name, None)
        if not inspect.isclass(schema) or not issubclass(schema, transformations_schema.Transformation):
            raise BaseException(f"Transformation `{name}` is not supported")
        transformations[idx] = schema().load(trf)
    return transformations


def filter_latest_scd_type2_change_with_data_date(
    df: DataFrame, key_cols: List[str], partition_date: datetime.date
) -> DataFrame:
    """
    Filter and get latest change per key_cols within partition_date
    :param df:
    :param key_cols:
    :param partition_date:
    :return:
    """
    window = Window.partitionBy(key_cols).orderBy(F.desc(DsColumn.RECORD_CREATED_TIMESTAMP))

    return (
        df.filter(F.to_date(DsColumn.RECORD_CREATED_TIMESTAMP, "yyyy-MM-dd") <= F.lit(partition_date))
        .filter(F.to_date(DsColumn.RECORD_UPDATED_TIMESTAMP, "yyyy-MM-dd") > F.lit(partition_date))
        .withColumn("row_num", F.row_number().over(window))
        .filter("row_num = 1")
        .drop("row_num")
    )