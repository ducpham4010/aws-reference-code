from datetime import datetime, timedelta

from pyspark.sql.session import SparkSession
from typing import Union

from da_ded.exceptions import BaseException
from da_ded.generic_modules.models.job_input import JobInput, GlueCatalog
from da_ded.generic_modules.readers.base import BaseReader
from da_ded.tools.glue_utils import get_partitions

DEFAULT_PUSH_DOWN_PREDICATE = ""
DATA_DATE_FIELD_NAME = "data_date"
DATA_DATE_TYPE_FIELD_NAME = "data_date_type"
_TYPE_LIST = "list"
_TYPE_RANGE = "range"


class CatalogReader(BaseReader):
    data_date = "data_date"
    data_date_type = "data_date_type"

    def __init__(self, job_input: JobInput, spark: SparkSession, glue_context, job_config):
        super().__init__(job_input, spark, glue_context)
        self.job_config = job_config
        self.logger = glue_context.get_logger()

    @classmethod
    def get_push_down_predicate(cls, glue_catalog, params):
        data_date = params.get(DATA_DATE_FIELD_NAME, None)
        data_date_type = params.get(DATA_DATE_TYPE_FIELD_NAME, None)
        return _parse_push_down_predicate(glue_catalog, data_date, data_date_type)

    def read(self, fallback=False, predicate_value=None):
        try:
            database = self.job_input.glue_catalog.get("database")
            table = self.job_input.glue_catalog.get("table")
            partition_column = self.job_input.glue_catalog.get("partition_column")
            params = self.job_config.params if self.job_config else {}
            push_down_predicate = (
                _format_push_down_predicate(predicate_value, partition_column)
                if predicate_value
                else self.get_push_down_predicate(self.job_input.glue_catalog, params)
            )
            transformation_ctx = self.job_input.glue_catalog["transformation_ctx"]
            additional_options = self.job_input.glue_catalog.get("additional_options", {})
            print(
                f"read catalog: database: {database},\n"
                f" table: {table}, \n"
                f"push_down_predicate: {push_down_predicate}, \n"
                f" additional_options: {additional_options}"
            )

            df = self.glue_context.create_dynamic_frame.from_catalog(
                database=database,
                table_name=table,
                transformation_ctx=transformation_ctx,
                push_down_predicate=push_down_predicate,
                additional_options=additional_options,
            ).toDF()
            print(f"Done read catalog, database: {database}, table: {table}")
            if not fallback and len(df.head(1)) == 0:
                if self.job_input.glue_catalog.get("fallback") is not None:
                    nearest_predicate = _get_fallback_predicate(self.job_input.glue_catalog, params)
                    return self.read(fallback=True, predicate_value=nearest_predicate)
                return df
            return df
        # TODO: be more specific, check what exceptions can be thrown
        except Exception as exp:  # NOSONAR
            if self.job_input.glue_catalog["empty_allow"]:
                return None
            raise exp


def _get_fallback_predicate(glue_catalog_read_params: Union[dict, GlueCatalog], params):
    database = glue_catalog_read_params.get("database")
    table = glue_catalog_read_params.get("table")
    partition_column = glue_catalog_read_params.get("partition_column")
    cur_predicate = params.get("data_date")
    if not cur_predicate:
        raise BaseException(f"not support get nearest predicate for {params}")
    if type(cur_predicate) is not str and (isinstance(cur_predicate, list) and len(cur_predicate) > 1):
        raise BaseException(f"not support get nearest predicate for {params}")
    max_offset = glue_catalog_read_params.get("fallback")["max_offset"]
    offset_date = datetime.strptime(cur_predicate, "%Y-%m-%d") - timedelta(days=max_offset)
    expression = f"{partition_column} >= '{offset_date.date()}'  and  {partition_column} <= '{cur_predicate}'"
    partitions_list = get_partitions(db_name=database, table_name=table, Expression=expression)
    if len(partitions_list):
        sorted_partition_list = sorted(partitions_list, key=lambda d: d["location"])
        nearest_partition_value = sorted_partition_list[-1]["values"][-1]
        return nearest_partition_value
    raise BaseException(f"not found any nearest partition {params}")


def _parse_push_down_predicate(glue_catalog_read_params: Union[dict, GlueCatalog], data_date, data_date_type):
    push_down_predicate = glue_catalog_read_params.get("push_down_predicate", DEFAULT_PUSH_DOWN_PREDICATE)
    enable_push_down_predicate = glue_catalog_read_params.get("enable_push_down_predicate", False)
    partition_column = glue_catalog_read_params.get("partition_column")
    expect_param = glue_catalog_read_params.get("expect_param", None)
    extend_range = glue_catalog_read_params.get("extend_range")
    # data_date is string, return 1 item
    if len(push_down_predicate):
        return push_down_predicate
    if not enable_push_down_predicate:
        return DEFAULT_PUSH_DOWN_PREDICATE
    if data_date:
        return _parse_predicate_from_data_date(data_date, data_date_type, partition_column, expect_param, extend_range)
    return DEFAULT_PUSH_DOWN_PREDICATE


def _parse_predicate_from_data_date(  # NOSONAR
    data_date: Union[str, None],
    data_date_type: Union[str, None],
    partition_column: str,
    expect_param: str,
    extend_range=0,
):
    if isinstance(data_date, str) and len(data_date):
        if expect_param == DATA_DATE_FIELD_NAME and extend_range == 0:
            return _format_push_down_predicate(data_date, partition_column)
        if expect_param == DATA_DATE_FIELD_NAME and extend_range != 0:
            range_date = [data_date,data_date]
            return _format_range_pushdown_predicates(range_date, partition_column, extend_range)
    elif isinstance(data_date, list):
        if len(data_date) == 0:
            raise BaseException("data_date must have at least 1 item")
        # data_date is single type
        if data_date_type is None:
            return _format_push_down_predicate(data_date[0], partition_column)
        # data_date is list type
        elif data_date_type == _TYPE_LIST:
            push_down_predicate = ""
            for index, the_date in enumerate(data_date):
                push_down_predicate += f"{_format_push_down_predicate(the_date, partition_column)}"
                if index < len(data_date) - 1:
                    push_down_predicate += " or "
            return push_down_predicate
        # data_date is range type
        elif data_date_type == _TYPE_RANGE:
            if len(data_date) != 2:
                raise BaseException("only support 1 range: [start- end]")
            the_date_range_list = _format_range_pushdown_predicates(data_date, partition_column, extend_range)
            return the_date_range_list
        else:
            return DEFAULT_PUSH_DOWN_PREDICATE


def _format_range_pushdown_predicates(range: list, partition_column="ds_partition_date", extend_range=0):
    start_range = range[0]
    end_range = range[1]
    if extend_range:
        start_range = datetime.strptime(start_range, "%Y-%m-%d") - timedelta(days=extend_range)
        start_range = start_range.date()
    return f"{partition_column} >= '{start_range}' and {partition_column} <= '{end_range}'"


def _format_push_down_predicate(push_down_predicate_value, partition_column="ds_partition_date"):
    return f"{partition_column} == '{push_down_predicate_value}'"