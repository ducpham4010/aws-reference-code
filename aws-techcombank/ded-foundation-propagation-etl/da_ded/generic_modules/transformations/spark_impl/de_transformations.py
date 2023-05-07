from datetime import datetime, date, timedelta
from decimal import getcontext, Decimal
import calendar

import pyspark.sql.functions as F
from dateutil import parser
from dateutil.relativedelta import relativedelta
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    DateType,
    StringType,
    MapType,
    ArrayType,
    IntegerType,
    LongType,
    BooleanType,
    ShortType,
    StructType,
    DecimalType,
    StructField,
)
from pyspark.sql.window import Window

from da_ded.exceptions import BaseException
from da_ded.generic_modules.models.base_models import DotDict
from da_ded.marking_data.rules.mark_by_combine_with_const import MarkByCombineWithConst
from da_ded.marking_data.rules.mark_by_substring import MarkBySubString
from .constants import SPARK_TIMEZONE_CFG, SparkSQLDataType
from .udfs import validate_gso_email_address
from .utils import get_columns_from_column_patterns, spark_convert_date_to_ds_partition_date_format, to_snake_case
from ...models.constants import DsColumn, RecordChangeType
from ...models.job_config import get_raw_partition_date


def lower_case_column_name(df, trf):
    trf = DotDict(trf)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        df = df.withColumnRenamed(col, col.lower())
    return df


def snake_case_column_name(df, trf):
    trf = DotDict(trf)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        df = df.withColumnRenamed(col, to_snake_case(col))
    return df


def cast_to_lower_by_list(df, trf):
    trf = DotDict(trf)
    for col in trf.column_list:
        df = df.withColumnRenamed(col, col.lower())
    return df


def rename_column(df, trf):
    trf = DotDict(trf)
    for col in trf.rename_list:
        df = df.withColumnRenamed(col["origin"], col["new"])
    return df


def column_name_standardize(df, trf):
    trf = DotDict(trf)
    pattern = trf.pattern
    for col in df.columns:
        col: str
        df = df.withColumnRenamed(col, col.rstrip(f"{pattern}"))
    return df


def create_partition_by_column(df, trf):
    trf = DotDict(trf)
    for rule in trf.rule_list:
        if rule["type"] == "month_string":
            df = df.withColumn(rule["partition_column"], F.concat(rule["source_column"], F.lit("01")))
            df = df.withColumn(
                rule["partition_column"],
                F.date_format(F.to_date(rule["partition_column"], "yyyyMMdd"), "yyyy-MM-dd"),
            )
        elif rule["type"] == "timestamp":
            df = df.withColumn(
                rule["partition_column"],
                F.date_format(F.to_date(rule["source_column"]), "yyyy-MM-dd"),
            )
        elif rule["type"] == "date":
            df = df.withColumn(
                rule["partition_column"],
                spark_convert_date_to_ds_partition_date_format(rule["source_column"]),
            )
        else:
            raise BaseException(f"{rule['type']} is not support for get partition")
    return df


def _resolve_args(value):
    if type(value) is dict and "__function_name__" in value:
        function_name = value["__function_name__"]
        func = getattr(F, function_name, None)
        if not callable(func):
            raise BaseException(f"{function_name} is not a function in pyspark.sql.functions")
        return func(
            *_resolve_args(value.get("args", [])),
            **_resolve_args(value.get("kwargs", {})),
        )
    elif type(value) is list:
        for idx, v in enumerate(value):
            value[idx] = _resolve_args(v)
        return value
    elif type(value) is dict:
        for k, v in value.items():
            value[k] = _resolve_args(v)
        return value
    else:
        return value


def pyspark_df_function(df, trf):
    trf = DotDict(trf)
    func = getattr(df, trf.function_name, None)
    if not callable(func):
        raise BaseException(f"{trf.function_name} is not a function in pyspark.sql.dataframe.DataFrame")
    return func(*_resolve_args(trf.args), **_resolve_args(trf.kwargs))


def get_row_with_max_value_for_each_group(df, trf):
    trf = DotDict(trf)
    w = Window().partitionBy(trf.cols_to_group_by).orderBy(*[F.desc(c) for c in trf.cols_to_get_max])
    df = df.withColumn("rn", F.row_number().over(w))
    return df.where(df["rn"] == 1).drop("rn")


def filter_with_regex_pattern(df, trf):
    trf = DotDict(trf)
    if trf.inverse:
        return df.filter(~F.col(trf.column).rlike(trf.pattern))
    return df.filter(F.col(trf.column).rlike(trf.pattern))


def cast_string_to_timestamp(df, trf):
    trf = DotDict(trf)
    backup_tz_conf = trf.spark_env.spark.conf.get(SPARK_TIMEZONE_CFG)
    trf.spark_env.spark.conf.set(SPARK_TIMEZONE_CFG, trf.timezone)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        df = df.withColumn(col, F.to_timestamp(df[col], format=trf.timestamp_format).alias(col))
    trf.spark_env.spark.conf.set(SPARK_TIMEZONE_CFG, backup_tz_conf)
    return df


def cast_string_to_unix_timestamp(df, trf):
    trf = DotDict(trf)
    backup_tz_conf = trf.spark_env.spark.conf.get(SPARK_TIMEZONE_CFG)
    trf.spark_env.spark.conf.set(SPARK_TIMEZONE_CFG, trf.timezone)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        df = df.withColumn(col, F.unix_timestamp(df[col], format=trf.timestamp_format).alias(col))
    trf.spark_env.spark.conf.set(SPARK_TIMEZONE_CFG, backup_tz_conf)
    return df


def cast_string_to_timestamp_type(df, trf):
    trf = DotDict(trf)
    backup_tz_conf = trf.spark_env.spark.conf.get(SPARK_TIMEZONE_CFG)
    trf.spark_env.spark.conf.set(SPARK_TIMEZONE_CFG, trf.timezone)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        df = df.withColumn(
            col,
            F.to_timestamp(df[col], format=trf.timestamp_format).cast("timestamp").alias(col),
        )
    trf.spark_env.spark.conf.set(SPARK_TIMEZONE_CFG, backup_tz_conf)
    return df


def cast_string_to_date(df, trf):
    trf = DotDict(trf)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        df = df.withColumn(col, F.to_date(df[col], format=trf.date_format).alias(col))
    return df


def copy_column(df, trf):
    trf = DotDict(trf)
    return df.withColumn(trf.new_column, df[trf.copied_column])


def add_current_date_column(df, trf):
    trf = DotDict(trf)
    return df.withColumn(trf.column_name, F.current_date())


def add_current_timestamp_column(df, trf):
    trf = DotDict(trf)
    return df.withColumn(trf.column_name, F.current_timestamp())


def cast_string_to_dateformat(df, trf):
    trf = DotDict(trf)
    for col in get_columns_from_column_patterns(df, trf.column_paterns):
        df = df.withColumn(col, F.date_format(df[col], format=trf.date_format).alias(col))
    return df


def cast_to_decimaltype(df, trf):
    trf = DotDict(trf)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        df = df.withColumn(
            col,
            F.regexp_replace(col, ",", "").cast(f"decimal({trf.precision},{trf.scale})"),
        )
    return df


def cast_to_decimaltype_v2(df, trf):
    trf = DotDict(trf)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        df = df.withColumn(
            col,
            F.regexp_replace(col, ",", "").cast(f"decimal({trf.precision},{trf.scale})"),
        )
    return df

def cast_to_decimaltype_v3(df, trf):
    trf = DotDict(trf)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        df = df.withColumn(
            col,
            F.regexp_replace(col, ",", "").cast(f"decimal({trf.precision},{trf.scale})"),
        )
    return df


def cast_to_integertype(df, trf):
    trf = DotDict(trf)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        df = df.withColumn(col, df[col].cast(IntegerType()))
    return df


def cast_to_shorttype(df, trf):
    trf = DotDict(trf)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        df = df.withColumn(col, df[col].cast(ShortType()))
    return df


def cast_to_longtype(df, trf):
    trf = DotDict(trf)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        df = df.withColumn(col, df[col].cast(LongType()))
    return df


def cast_to_stringtype(df, trf):
    trf = DotDict(trf)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        df = df.withColumn(col, df[col].cast(StringType()))
    return df


def trim_all_trailing_space(df, trf):
    trf = DotDict(trf)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        df = df.withColumn(col, F.trim(df[col]))
    return df


def drop_all_column_except(df, trf):
    trf = DotDict(trf)
    columns = df.columns
    except_column_list = trf.column_list
    res = [i for i in columns if i not in except_column_list]
    df = df.drop(*res)
    return df


def distinct_dataframe(df, trf):
    trf = DotDict(trf)
    df = df.distinct()
    return df


def calculate_hash_index(df, trf):
    trf = DotDict(trf)
    columns = df.columns
    exclusive_columns = trf.exclusive_columns
    if len(exclusive_columns):
        res = [i for i in columns if i not in exclusive_columns]
    else:
        res = df.columns
    df = df.withColumn(f"{trf.column_name}", F.sha2(F.concat_ws("||", *res), 256))
    return df


def add_partition_date_column(df, trf):
    trf = DotDict(trf)
    df = df.withColumn(
        f"{trf.column_name}",
        F.to_date(F.to_utc_timestamp(F.col("ds_record_created_timestamp"), "UTC+7")),
    )
    return df


def add_ds_partition_date_by_data_date(df, trf):
    trf = DotDict(trf)
    date_regex = trf.date_regex
    date_format = trf.date_format
    data_date = get_raw_partition_date(trf.job_config, date_regex, date_format)
    offset = int(trf.offset_date)
    data_date = data_date + timedelta(days=offset)
    df = df.withColumn(f"{trf.column_name}", F.lit(data_date.date()))
    return df


def drop_record_if_none_in_primary_column(df, trf):
    trf = DotDict(trf)

    primary_columns = trf.primary_columns
    for each_column in primary_columns:
        df = df.filter(df[each_column].isNotNull())
    return df


def scd_type2_dedup_by_hash_index(df, trf):
    trf = DotDict(trf)
    hash_index_field = trf.hash_index
    base_field = trf.base_field
    created_field = trf.created_field
    primary_columns = trf.primary_columns
    window_spec = Window.partitionBy(*primary_columns).orderBy(f"{base_field}")
    df = df.withColumn(
        "dedupe",
        F.when(
            F.lag(hash_index_field).over(window_spec) == F.col(hash_index_field),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )
    df = df.filter(F.col("dedupe") == 0).drop("dedupe")
    df = df.withColumn(f"{created_field}", F.col(f"{base_field}")).drop(base_field)
    return df


def scd_type2_fill_record_update_time(df, trf):
    """

    :param df:
    :param trf:
        trf.partition_key :
        trf.created_field:
        trf.updated_field:
    :return:
    """
    "cms customer => primary column = customer_id"
    "techialfield: ds_record_created_timestamp"
    import pytz

    vn_tz = pytz.timezone("Asia/Ho_Chi_Minh")
    max_updated_timestamp = 32530525200

    trf = DotDict(trf)
    primary_column = trf.partition_key
    created_field = trf.created_field
    updated_field = trf.updated_field

    window_spec = Window.partitionBy(*primary_column).orderBy(f"{created_field}")
    df = df.withColumn(f"{updated_field}", F.lead(f"{created_field}", 1).over(window_spec))
    df = df.withColumn(
        f"{updated_field}",
        F.when(
            df[f"{updated_field}"].isNull(),
            F.lit(datetime.fromtimestamp(max_updated_timestamp, vn_tz)).cast("timestamp"),
        ).otherwise(df[f"{updated_field}"]),
    )
    return df


def fill_record_change_type_1tm(df, trf):
    """

    :param df:
    :param trf:
    trf.
    :return:
    """
    trf = DotDict(trf)
    column_name = trf.column_name
    change_type = trf.change_type
    df = df.withColumn(f"{column_name}", F.lit(change_type))
    return df


def fill_record_status_1tm(df, trf):
    """

    :param df:
    :param trf:
    trf.
    :return:
    """
    import pytz

    vn_tz = pytz.timezone("Asia/Ho_Chi_Minh")
    max_updated_timestamp = 32530525200

    trf = DotDict(trf)
    column_name = trf.column_name
    base_field = trf.base_field
    df = df.withColumn(
        f"{column_name}",
        F.when(
            df[f"{base_field}"] == datetime.fromtimestamp(max_updated_timestamp, vn_tz),
            1,
        ).otherwise(0),
    )
    return df


def cast(df, trf):
    trf = DotDict(trf)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        df = df.withColumn(col, df[col].cast(trf.target_type))
    return df


def add_constant_column(df, trf):
    trf = DotDict(trf)
    if trf.skip_if_exist and trf.column_name in df.columns:
        return df
    return df.withColumn(trf.column_name, F.lit(trf.value).cast(trf.target_type))


def fill_col_if_not_exist(df, trf):
    trf = DotDict(trf)
    fill_column = trf.column
    if fill_column not in df.columns:
        df = df.withColumn(fill_column, F.lit(None).cast(StringType()))
    return df


def union_with_golden_if_existed(df, trf):
    from da_ded.generic_modules.readers.factory import create_reader
    from da_ded.generic_modules.models.job_input import JobInput
    from awsglue.context import GlueContext
    from pyspark.sql import DataFrame

    trf = DotDict(trf)
    technical_fields = [
        "ds_record_created_timestamp",
        "ds_record_updated_timestamp",
        "ds_record_status",
        "ds_record_change_type",
    ]

    golden_catalog = trf.golden_catalog
    glue_context: GlueContext = trf.glue_context
    logger = glue_context.get_logger()
    spark = glue_context.spark_session
    reader = create_reader(JobInput(golden_catalog), spark, glue_context)
    golden_df = reader.read()
    if isinstance(golden_df, DataFrame) and len(golden_df.head(1)) > 0:
        logger.info("Process golden union")
        golden_df = golden_df.cache()
        golden_df.count()
        golden_df = golden_df.drop(*technical_fields)
        golden_df = golden_df.withColumnRenamed("ds_partition_date", "process_date")
        df = df.unionByName(golden_df)
    else:
        logger.info("None Process golden union")
    logger.info("Process golden union done")
    return df


def _transform_single_value(f, datatype, kwargs: dict = None):
    def _transform_single(value):
        if value is None:
            return None
        if datatype == SparkSQLDataType.STRING:
            masked_value = f(value)
        elif datatype == SparkSQLDataType.INTEGER or datatype == SparkSQLDataType.BIG_INTEGER:
            masked_value = int(f(value))
        elif datatype == SparkSQLDataType.DATE or datatype == SparkSQLDataType.TIMESTAMP:
            masked_value = datetime.strptime(f(value), kwargs["datetime_format"])
        elif datatype == SparkSQLDataType.DECIMAL:
            getcontext().prec = kwargs["fractional"]
            masked_value = Decimal(f(value))
        else:
            raise ValueError('The type {} is not currently supported'.format(datatype))
        return masked_value

    return _transform_single


def _transform_dict_value(f, datatype, kwargs: dict = None):
    def _transform_dict(value):
        try:
            if value is None:
                return None
            masked_value = {}
            for k, v in value.items():
                if v is None:
                    masked_value[k] = None
                elif datatype == SparkSQLDataType.STRING:
                    masked_value[k] = f(v)
                elif datatype == SparkSQLDataType.INTEGER or datatype == SparkSQLDataType.BIG_INTEGER:
                    masked_value[k] = int(f(v))
                elif datatype == SparkSQLDataType.DATE or datatype == SparkSQLDataType.TIMESTAMP:
                    masked_value[k] = datetime.strptime(f(v), kwargs["datetime_format"])
                elif datatype == SparkSQLDataType.DECIMAL:
                    getcontext().prec = kwargs["fractional"]
                    masked_value[k] = Decimal(f(v))
                else:
                    raise ValueError('The type map<string,{}> is not currently supported'.format(datatype))
            return masked_value
        except Exception as e:
            raise e

    return _transform_dict


def _transform_list_value(f):
    def _transform_list(value):
        try:
            return None if value is None else list(map(f, value))
        except Exception as e:
            raise e

    return _transform_list


def _dict_to_date_wrapper(python_date_format_list):
    # pypark 2.4.3 does not support tranforms and transform_values functions, need to use udf to handle map/array type
    # https://stackoverflow.com/a/67625749
    # https://stackoverflow.com/a/60504979
    def _dict_to_date(value):
        if value is None:
            return None
        for k, v in value.items():
            for fmt in python_date_format_list:
                try:
                    if fmt == "dateutil_parser":
                        value[k] = parser.parse(v).date()
                    else:
                        value[k] = datetime.strptime(v, fmt).date()
                except Exception:
                    pass
            if type(value[k]) != date:
                value[k] = None
        return value

    return _dict_to_date


def _list_to_date_wrapper(python_date_format_list):
    def _list_to_date(value):
        if value is None:
            return None
        for idx, v in enumerate(value):
            for fmt in python_date_format_list:
                try:
                    if fmt == "dateutil_parser":
                        value[idx] = parser.parse(v).date()
                    else:
                        value[idx] = datetime.strptime(v, fmt).date()
                except Exception:
                    pass
            if type(value[idx]) != date:
                value[idx] = None
        return value

    return _list_to_date


def cast_to_date(df, trf):
    trf = DotDict(trf)
    err_msg = "Only `string`, `map(string, string)`, `array(string)` types are supported"
    col_types = dict(df.dtypes)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        col_type = col_types[col]
        if col_type == "string":
            if trf.date_format_list is None:
                raise BaseException("date_format_list is required for casting string type")
            df = df.withColumn(
                col,
                F.coalesce(*[F.to_date(df[col], format=fmt).alias(col) for fmt in trf.date_format_list]),
            )
        elif col_type == "map<string,string>":
            if trf.python_date_format_list is None:
                raise BaseException("python_date_format_list is required for casting map type")
            python_date_format_list = trf.python_date_format_list
            spark_udf = F.udf(
                _dict_to_date_wrapper(python_date_format_list), returnType=MapType(StringType(), DateType())
            )
            df = df.withColumn(col, spark_udf(df[col]))
        elif col_type == "array<string>":
            python_date_format_list = trf.python_date_format_list
            spark_udf = F.udf(_list_to_date_wrapper(python_date_format_list), returnType=ArrayType(DateType()))
            df = df.withColumn(col, spark_udf(df[col]))
        else:
            raise BaseException(err_msg)
    return df


def add_data_date_from_event(df, trf):
    trf = DotDict(trf)
    data_date_str = trf.job_config.params["event"]["date"]
    data_date = parser.parse(data_date_str).date()
    df = df.withColumn(f"{trf.column_name}", F.lit(data_date))
    return df


def add_constant_column_from_cob_date(df, trf):
    trf = DotDict(trf)
    cob_date = trf.job_config.cob_date
    if cob_date is None:
        raise BaseException(
            "cob_date field does not exist in job input. "
            + "Make sure you are running ETL job for a COB table, and not using date range."
        )
    # reformat cob_date: "20220212" -> "2022-02-12"
    cob_date = parser.parse(cob_date).strftime("%Y-%m-%d")
    df = df.withColumn(f"{trf.column_name}", F.lit(cob_date))
    return df


def parse_json_cols(df, trf):
    trf = DotDict(trf)
    clnm = trf.column_name
    clprs = trf.column_parse
    df = df.withColumn(f"{clnm}", F.get_json_object(F.col(f"{clprs}"), f"$.{clnm}"))
    return df


def copy_column_ifnull(df, trf):
    trf = DotDict(trf)
    df = df.withColumn(
        f"{trf.new_column}",
        F.when(df[trf.copied_column_1].isNull(), df[trf.copied_column_2]).otherwise(df[trf.copied_column_1]),
    )
    return df


def boolean_to_int(df, trf):
    trf = DotDict(trf)
    df = df.withColumn(
        f"{trf.column_name}",
        F.when(F.upper(df[trf.column_name]) == "TRUE", 1).when(F.upper(df[trf.column_name]) == "FALSE", 0),
    )
    return df


def cast_epoch_ms_to_timestamp(df, trf):
    trf = DotDict(trf)
    backup_tz_conf = trf.spark_env.spark.conf.get(SPARK_TIMEZONE_CFG)
    trf.spark_env.spark.conf.set(SPARK_TIMEZONE_CFG, trf.timezone)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        df = df.withColumn(col, F.to_timestamp(df[col] / 1000).cast("timestamp").alias(col))
    trf.spark_env.spark.conf.set(SPARK_TIMEZONE_CFG, backup_tz_conf)
    return df


def cast_unix_timestamp_to_timestamp(df, trf):
    trf = DotDict(trf)
    backup_tz_conf = trf.spark_env.spark.conf.get(SPARK_TIMEZONE_CFG)
    trf.spark_env.spark.conf.set(SPARK_TIMEZONE_CFG, trf.timezone)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        df = df.withColumn(col, F.from_unixtime(df[col]).cast("timestamp").alias(col))
    trf.spark_env.spark.conf.set(SPARK_TIMEZONE_CFG, backup_tz_conf)
    return df


def cast_unix_timestamp_to_timestamp_v2(df, trf):
    trf = DotDict(trf)
    backup_tz_conf = trf.spark_env.spark.conf.get(SPARK_TIMEZONE_CFG)
    trf.spark_env.spark.conf.set(SPARK_TIMEZONE_CFG, trf.timezone)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        df = df.withColumn(col, F.from_unixtime(df[col] / 1000).cast("timestamp").alias(col))
    trf.spark_env.spark.conf.set(SPARK_TIMEZONE_CFG, backup_tz_conf)
    return df


def cast_to_booleantype(df, trf):
    trf = DotDict(trf)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        df = df.withColumn(col, df[col].cast(BooleanType()))
    return df


def cast_to_arraytype(df, trf):
    trf = DotDict(trf)
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        df = df.withColumn(col, F.split(F.regexp_replace(df[col], r"(^\[)|(\]$)", ""), ","))
    return df


def structaccount_balance(df, trf):
    schema = ArrayType(
        StructType(
            [
                StructField("Balance", DecimalType(21, 6), True),
                StructField("AccountId", StringType(), True),
                StructField("AsciiFormula", StringType(), True),
                StructField("EntryMethodId", StringType(), True),
                StructField("OriginBalance", DecimalType(21, 6), True),
                StructField("OriginRounding", IntegerType(), True),
            ]
        )
    )
    trf = DotDict(trf)
    clnm = trf.column_name
    df = df.withColumn(f"{clnm}", F.from_json(F.col(f"{clnm}"), schema))
    return df


def structstatement_constant(df, trf):
    schema = ArrayType(
        StructType(
            [
                StructField("Id", LongType(), True),
                StructField("Value", StringType(), True),
            ]
        )
    )
    trf = DotDict(trf)
    clnm = trf.column_name
    df = df.withColumn(f"{clnm}", F.from_json(F.col(f"{clnm}"), schema))
    return df


def get_processing_date(df, trf):
    trf = DotDict(trf)
    clnm = trf.column_name
    df = df.withColumn(f"{clnm}", F.current_timestamp())
    return df


def from_uat_date(df, trf):
    # For Uat Job - remove after
    trf = DotDict(trf)
    clnm = trf.column_name
    df = df.withColumn(
        f"{clnm}",
        F.to_timestamp(F.lit("2021-01-01 17:00:00.000"), "yyyy-MM-dd HH:mm:ss.SSS"),
    )
    return df


def split_string_get_element(df, trf):
    trf = DotDict(trf)
    df = df.withColumn(
        f"{trf.target_column}", F.element_at(F.split(trf.source_column, trf.split_pattern), int(trf.extract_index))
    )
    return df


def cdc_1_3_scd2_columns(df: DataFrame, trf: dict) -> DataFrame:
    tmp_cols = []

    for ts_column in trf["timestamp_columns"]:
        tmp_col = f"_tmp_{ts_column}"

        df = df.withColumn(tmp_col, F.to_timestamp(F.col(ts_column))).withColumn(
            tmp_col,
            F.when(
                F.col(tmp_col).isNotNull() & (F.year(F.col(tmp_col)) >= F.lit(trf["inf_year"])), F.lit(None)
            ).otherwise(F.col(tmp_col)),
        )

        tmp_cols.append(tmp_col)

    df = (
        df.withColumn(DsColumn.RECORD_CREATED_TIMESTAMP, F.coalesce(*tmp_cols))
            .withColumn(DsColumn.CDC_INDEX, F.lit(-1).cast("long"))
            .withColumn(DsColumn.RECORD_CHANGE_TYPE, F.lit(RecordChangeType.CDC_UPDATE))
    ).drop(*tmp_cols)

    return df


def add_pk_from_columns(df, trf):
    trf = DotDict(trf)
    columns = df.columns
    include_columns = trf.include_columns
    if len(include_columns):
        res = [i for i in columns if i in include_columns]
    else:
        raise Exception("include_columns must not be null")
    df = df.withColumn(f"{trf.column_name}", F.sha2(F.concat_ws("||", *res), 256))
    return df


def struct_limitclassaggregation(df, trf):
    schema = ArrayType(
        StructType(
            [
                StructField("entityid", StringType(), True),
                StructField("limitclass", StringType(), True),
                StructField("approvedaggregatedlimit", DecimalType(), True),
                StructField("limitclassaggregationId", DecimalType(), True),
                StructField("proposedaggregatedlimit", DecimalType(), True),
                StructField("groupapprovedaggregatedlimit", DecimalType(), True),
                StructField("groupproposedaggregatedlimit", DecimalType(), True),
            ]
        )
    )
    trf = DotDict(trf)
    clnm = trf.column_name
    df = df.withColumn(f"{clnm}", F.from_json(F.col(f"{clnm}"), schema))
    return df


def struct_primaryborrower(df, trf):
    schema = ArrayType(
        StructType([StructField("EntityId", LongType(), True), StructField("LongName", StringType(), True)])
    )
    trf = DotDict(trf)
    clnm = trf.column_name
    df = df.withColumn(f"{clnm}", F.from_json(F.col(f"{clnm}"), schema))
    return df


def left_join_with_golden_if_existed(df, trf):
    from da_ded.generic_modules.readers.factory import create_reader
    from da_ded.generic_modules.models.job_input import JobInput
    from awsglue.context import GlueContext
    from pyspark.sql import DataFrame

    trf = DotDict(trf)

    golden_catalog = trf.golden_catalog
    glue_context: GlueContext = trf.glue_context
    logger = glue_context.get_logger()
    spark = glue_context.spark_session
    reader = create_reader(JobInput(golden_catalog), spark, glue_context)
    golden_df = reader.read()
    if isinstance(golden_df, DataFrame) and len(golden_df.head(1)) > 0:
        logger.info("Process golden union")
        common_cols = set(df.columns).intersection(set(golden_df.columns))
        if len(common_cols) > 0:
            for col in common_cols:
                golden_df = golden_df.withColumnRenamed(col, col + "_2")
        if trf.golden_subset_cols:
            keep_cols = [i for i in golden_df.columns if i in trf.golden_subset_cols]
            golden_df = golden_df.select(*keep_cols)
        if trf.golden_dedup_keys:
            golden_df = golden_df.dropDuplicates(trf.golden_dedup_keys)
        golden_df = golden_df.cache()
        df = df.join(golden_df, df[trf.left_on] == golden_df[trf.right_on], "left")
    else:
        logger.info("Golden table not existed")
    logger.info("Process left join with golden done")
    return df


def drop_columns(df, trf):
    trf = DotDict(trf)
    column_list = trf.column_list
    df = df.drop(*column_list)
    return df


def struct_bcde_entityhierarchy_from_to(df, trf):
    schema = ArrayType(
        StructType(
            [
                StructField("CountryOfInc", StringType(), True),
                StructField("EntityId", LongType(), True),
                StructField("EntityType", StringType(), True),
                StructField("LongName", StringType(), True),
            ]
        )
    )
    trf = DotDict(trf)
    clnm = trf.column_name
    df = df.withColumn(f"{clnm}", F.from_json(F.col(f"{clnm}"), schema))
    return df


def get_lst_mth_from_date(df, trf):
    trf = DotDict(trf)
    date_regex = trf.date_regex
    date_format = trf.date_format
    data_date = get_raw_partition_date(trf.job_config, date_regex, date_format)
    offset = int(trf.offset_date)
    data_date = data_date + relativedelta(months=offset)
    cst_date = data_date.date()
    df = df.withColumn(f"{trf.column_name}", F.lit(cst_date.strftime("%Y%m")))
    return df



def mark_by_combine_with_const(df, trf):
    trf = DotDict(trf)
    fields_list = trf.fields_lst
    field_id = trf.field_id
    const_value = trf.const_value

    col_types = dict(df.dtypes)
    for field_name in fields_list:
        col_type = col_types[field_name]
        tmp_field = "tmp_{}".format(field_name)
        df = df.withColumn(
            f"{tmp_field}",
            F.concat(F.lit(const_value), F.lit(" "), F.coalesce(F.col(field_id), F.lit("")))
            if field_id is not None
            else F.lit(const_value),
        )
        make_type = MarkByCombineWithConst()
        if col_type == "string":
            # df = df.withColumn(f"{field_name}", df.tmp_field)
            pass
        elif col_type == "map<string,string>":
            spark_udf = F.udf(make_type.mark_string_map_type, returnType=MapType(StringType(), StringType()))
            df = df.withColumn(field_name, spark_udf(F.col(field_name), F.col(f"{tmp_field}")))
        elif col_type == "array<string>":
            spark_udf = F.udf(make_type.mark_array_map_type, returnType=ArrayType(StringType()))
            df = df.withColumn(field_name, spark_udf(F.col(field_name), F.col(f"{tmp_field}")))
        else:
            err_msg = f"{col_type} is not support for marking type mark_by_combine_with_const"
            raise BaseException(err_msg)
    df = df.drop("tmp_field")
    return df


def mark_by_substring(df, trf):
    trf = DotDict(trf)
    fields_list = trf.fields_lst
    index = trf.index

    col_types = dict(df.dtypes)

    for field_name in fields_list:
        col_type = col_types[field_name]

        slice_parts = index.split(":")
        if len(slice_parts) != 2:
            raise Exception("Invalid input -- either no ':' or too many")
        else:
            try:
                _ = 0 if not slice_parts[0] else int(slice_parts[0])
                _ = 0 if not slice_parts[1] else int(slice_parts[1])
            except ValueError:
                # Invalid input, not a number
                raise Exception("Invalid slice input, not a number")
            else:
                make_type = MarkBySubString()
                spark_udf = None
                if col_type == "string":
                    spark_udf = F.udf(make_type.mark_string_type, returnType=StringType())

                elif col_type == "map<string,string>":
                    spark_udf = F.udf(make_type.mark_string_map_type, returnType=MapType(StringType(), StringType()))

                elif col_type == "array<string>":
                    spark_udf = F.udf(make_type.mark_array_map_type, returnType=ArrayType(StringType()))

                if not spark_udf:
                    err_msg = f"{col_type} is not support for marking type mark_by_substring"
                    raise BaseException(err_msg)
                df = df.withColumn(field_name, spark_udf(F.col(field_name), F.lit(index)))
    return df


def validate_phone(df, trf):
    trf = DotDict(trf)
    clnm = trf.column_name
    df = df.withColumn(f"{clnm}", F.regexp_replace(F.col(clnm), '[^(0-9)+-]', ''))
    return df


def validate_email(df, trf):
    trf = DotDict(trf)
    clnm = trf.column_name
    df = df.withColumn(f"{clnm}", validate_gso_email_address(clnm))
    return df


def drop_duplicate_primarykey_order_by(df, trf):
    trf = DotDict(trf)
    primary = trf.primary_column
    col_order = trf.col_order
    window = Window.partitionBy(primary).orderBy(
        F.col(col_order).desc()
    )
    df = df.withColumn("_row_number", F.row_number().over(window)). \
        filter(F.col("_row_number") == 1).drop("_row_number")
    return df


def sub_string(df, trf):
    trf = DotDict(trf)
    columns = trf.columns
    index = trf.index
    slice_parts = index.split(":")
    for col in columns:
        df = df.withColumn(col, F.substring(df[col], int(slice_parts[0]), int(slice_parts[1])))
    return df


def to_df(df, trf):
    trf = DotDict(trf)
    columns = trf.columns
    df = df.toDF(*columns)
    return df


def decode_base64_payload(df, trf):
    from pyspark.sql.functions import unbase64
    trf = DotDict(trf)
    payload_key = trf.column_patterns
    df_output = df.withColumn(payload_key, unbase64(df[payload_key]).cast("string"))
    return df_output


def extract_json_payload(df, trf):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, MapType
    from pyspark.sql.functions import explode
    trf = DotDict(trf)
    payload_key = trf.column_patterns
    pay_load = df.withColumn("pay_load", F.from_json(F.col(payload_key), F.lit("map<string,string>")))
    payload_df = pay_load.withColumn("pay_load", pay_load.pay_load.cast(MapType(StringType(), StringType())))
    payload_df_size = payload_df.filter(F.size(F.col("pay_load")) > 1)
    payload_df_select = payload_df_size.select(payload_df_size.KAFKA_OFFSET,payload_df_size.pay_load, explode(payload_df_size.pay_load))
    payload_df_key = payload_df_select.select(F.col('KAFKA_OFFSET'),F.col('value'))
    payload_df_key_temp = payload_df_key.withColumn("value_map",
                                                    F.from_json(F.col("value"), F.lit("map<string,string>")))
    payload_df_map = payload_df_key_temp.withColumn("value_map", payload_df_key_temp.value_map.cast(
        MapType(StringType(), StringType())))
    payload_df_explode = payload_df_map.select(payload_df_map.KAFKA_OFFSET,payload_df_map.value_map, explode(payload_df_map.value_map))
    payload_df_before_final = payload_df_explode.select(F.col('KAFKA_OFFSET'),F.col('key'), F.col('value'))
    df_output = payload_df_before_final.groupBy("KAFKA_OFFSET").pivot("key").agg(F.first("value"))
    df_output = df_output.withColumn("operation",
                                     F.when(df_output["operation"].isin("load"), F.lit("CDC_INSERT"))
                                     .when(df_output["operation"].isin("insert"), F.lit("CDC_INSERT"))
                                     .when(df_output["operation"].isin("update"), F.lit("CDC_UPDATE"))
                                     .when(df_output["operation"].isin("delete"), F.lit("CDC_DELETE"))
                                     .otherwise(df_output["operation"]))

    for col in df_output.columns:
        df_output = df_output.withColumn(col, df_output[col].cast(StringType()))
    return df_output


def explode_map_columns(df, trf):
    trf = DotDict(trf)

    def _construct_read_input(reader_type: str,
                              catalog_database: str = None,
                              catalog_table: str = None,
                              raw_uri_list: list = None):
        read_input = {
            "name": "dwh_tbl_dic_t24",
            "reader_type": reader_type
        }
        if reader_type == "catalog":
            read_input["glue_catalog"] = {
                "database": catalog_database,
                "table": catalog_table,
                "enable_push_down_predicate": False,
                "empty_allow": True
            }
            read_input["serde"] = dict(format="PARQUET")
        elif reader_type == "raw":
            read_input["uri_list"] = raw_uri_list
            read_input["serde"] = {
                "format": "CSV",
                "multi_line": True
            }
        return read_input

    def _read_from_job_input(glue_context, job_input):
        from da_ded.generic_modules.readers.factory import create_reader
        from da_ded.generic_modules.models.job_input import JobInput
        from types import GeneratorType

        spark = glue_context.spark_session
        reader = create_reader(JobInput(job_input), spark, glue_context)

        res_df = reader.read()
        if isinstance(res_df, GeneratorType):
            res_df = next(res_df)
        return res_df

    def _values_mapper_func(input_df, column: str, mapping: dict):
        from itertools import chain
        mapping_expr = F.create_map([F.lit(x) for x in chain(*mapping.items())])
        return input_df.withColumn(
            column,
            F.coalesce(mapping_expr.__getitem__(F.col(column)), F.col(column))
        )

    def _tbl_dic_t24_processor(input_df,
                               t24_table_name: str,
                               multi_value_group: str,
                               sub_value_group: str = None,
                               column_names_mapper: dict = None):
        input_df = input_df.select([F.col(c).alias(c.lower()) for c in input_df.columns])
        input_df = input_df.filter(
            (F.col("table_name") == t24_table_name) &
            (F.col("status") == "Y") &
            (F.col("multi_value") == multi_value_group)
        )
        if sub_value_group is not None:
            input_df = input_df.filter(
                (F.col("sub_value").isNull()) |
                (F.col("sub_value") == sub_value_group)
            )
        if column_names_mapper is not None:
            input_df = _values_mapper_func(input_df, "field_name", column_names_mapper)
        else:
            input_df = input_df.withColumn("field_name", F.lower(F.col("field_name")))
        return input_df

    if not trf.use_groupings_from_tbl_dic_t24:
        multi_map_cols = trf.manual_groupings.get('multi_cols', [])
        sub_multi_map_cols = trf.manual_groupings.get('sub_multi_cols', [])
    else:
        tbl_dic_t24_load_details = DotDict(trf.tbl_dic_t24_load_details)
        read_input_params = {
            "reader_type": tbl_dic_t24_load_details.reader_type,
            "catalog_database": tbl_dic_t24_load_details.database,
            "catalog_table": tbl_dic_t24_load_details.table,
            "raw_uri_list": tbl_dic_t24_load_details.uri_list
        }
        tbl_dic_t24_read_input = _construct_read_input(**read_input_params)

        tbl_dic_t24_df = _read_from_job_input(trf.glue_context, tbl_dic_t24_read_input)

        if isinstance(tbl_dic_t24_df, DataFrame) and len(tbl_dic_t24_df.head(1)) > 0:
            filter_args = {
                "t24_table_name": tbl_dic_t24_load_details.t24_table_name,
                "multi_value_group": tbl_dic_t24_load_details.multi_value_group,
                "sub_value_group": tbl_dic_t24_load_details.sub_value_group,
                "column_names_mapper": tbl_dic_t24_load_details.column_names_mapper
            }
            tbl_dic_t24_df = _tbl_dic_t24_processor(tbl_dic_t24_df, **filter_args)

            tbl_dic_t24_cols = tbl_dic_t24_df.select("field_name", "sub_value").collect()
            multi_map_cols = [row.field_name for row in tbl_dic_t24_cols if row.sub_value is None]
            sub_multi_map_cols = [row.field_name for row in tbl_dic_t24_cols if row.sub_value is not None]

            if tbl_dic_t24_load_details.columns_selection:
                keep_cols = get_columns_from_column_patterns(df, tbl_dic_t24_load_details.columns_selection)
                multi_map_cols = [col for col in multi_map_cols if col in keep_cols]
                sub_multi_map_cols = [col for col in sub_multi_map_cols if col in keep_cols]
        else:
            raise BaseException("Table tbl_dic_t24 is EMPTY on load!")

    map_cols = multi_map_cols + sub_multi_map_cols
    df = df.alias("df") \
        .select(
        "df.*",
        F.array_distinct(
            F.concat(*[F.coalesce(F.map_keys(F.col(column)), F.array()) for column in map_cols])).alias("__temp_keys")
    )
    df = df.withColumn("__key", F.explode_outer(df.__temp_keys).alias("__key"))
    df = df.withColumn("pos", F.element_at(F.split(F.col("__key"), "\\."), 1).cast("int"))
    df = df.withColumn("sub_pos", F.element_at(F.split(F.col("__key"), "\\."), 2).cast("int"))
    for column in multi_map_cols:
        df = df.withColumn(column, F.element_at(F.col(column), F.concat(F.col("pos"), F.lit(".1"))))
    for column in sub_multi_map_cols:
        df = df.withColumn(column, F.element_at(F.col(column), F.col("__key")))
    df = df.drop(*["__temp_keys", "__key"])

    if trf.drop_nulls:
        df = df.select(
            [F.when(F.col(c) == "", None).otherwise(F.col(c)).alias(c) if c in map_cols else F.col(c) for c in
             df.columns])
        df = df.dropna(how='all', subset=list(map_cols))

    return df


def payload_handle_missing_fields(df, trf):
    trf = DotDict(trf)
    columns=trf.column_patterns
    if df and len(df.head(1)) != 0:
        for col in columns:
            if col not in df.columns:
                df = df.withColumn(col, F.lit(None).cast(StringType()))
    return df


def replace_value_in_map_type_column(df, trf):
    """
    replace value in a mapType column
    sample usage:
    {
        "name": "replace_value_in_map_type_column",
        "column_name": "account_title_1"
        "replace_list": [
            {
                  "to_replace": "TKL",
                  "value": "TGTT"
            }
        ]
    }
    """
    trf = DotDict(trf)
    column_name = trf.column_name

    for col in trf.replace_list:
        to_replace = col["to_replace"]
        value = col["value"]

        temp_df = df.withColumn(
            column_name,
            F.when(
                F.col(column_name).isNotNull(),
                F.regexp_replace(F.col(column_name).cast(StringType()), to_replace, value)
            )
        )

        df = (
            temp_df.withColumn(column_name, F.split(F.regexp_replace(column_name, "[\\{\\}]", ""), ","))
            .withColumn(column_name,
                F.map_from_entries(
                    F.expr(f"""transform({column_name}, e -> (trim(regexp_extract(e, '^(.*) ->',1)),trim(regexp_extract(e, '-> (.*)$',1))))""") # noqa
                )
            )
        )

        return df


def transform_category_account_salary_hist__cob(df, trf):
    """
    selective category transformations for only 1006 records
    """
    trf = DotDict(trf)
    column_name = trf.column_name

    df = df.withColumn(
        column_name,
        F.when(
            F.col(column_name) == 1006,
            F.lit("1001")
        ).otherwise(F.col(column_name))
    )

    return df

def get_last_day_of_month(df, trf):

    """
    To get Last date of the month bypassing month and year alone as a string. Eg:MM/yyyy
    """
    trf = DotDict(trf)
    column_name = trf.column_name
    date_format = trf.date_format
    df = df.withColumn(column_name, F.last_day(F.to_date(F.col(column_name), date_format)))
    return df


def json_string_to_map_string(df, trf):
    """
    convert columns json string to map(string, string)
    """

    trf = DotDict(trf)
    for column_name in trf.column_patterns:
        df = df.withColumn(column_name, F.from_json(F.col(column_name),  MapType(StringType(), StringType())))
    return df

def add_indentifier_for_scd_operation(df: DataFrame,trf):
    """
    Read the dataframe from raw zone and golden zone respectively
    Add fields so that T24SCDTYPE2 can process the data ds_record_created_timestamp,ds_record_change_type,
    ds_cdc_index, ds_record_source
    :param df:
    :return:
    """
    from da_ded.generic_modules.readers.factory import create_reader
    from da_ded.generic_modules.models.job_input import JobInput
    from awsglue.context import GlueContext
    from pyspark.sql import DataFrame
    from pyspark.sql.window import Window

    trf = DotDict(trf)

    # Read data from golden zone
    golden_catalog = trf.golden_catalog
    glue_context: GlueContext = trf.glue_context
    logger = glue_context.get_logger()
    spark = glue_context.spark_session
    reader = create_reader(JobInput(golden_catalog), spark, glue_context)
    golden_df = reader.read()

    #data_date_str = trf.job_config.params["event"]["date"]
    #data_date = parser.parse(data_date_str).date()
    partition_date = get_raw_partition_date(trf.job_config)
    data_date = partition_date.date()

    if data_date is None:
        raise BaseException("data_date is None")
    else:
        if isinstance(golden_df, DataFrame) and len(golden_df.head(1)) > 0:
            # Handling CDC_DELETE, CDC_UPDATE, CDC_INSERT for day-1
            df = (df.withColumn(DsColumn.RECORD_CHANGE_TYPE,
                                F.when(F.to_date(F.col("createddate"), "yyyy-MM-dd'T'HH:mm:ss.SSSS'Z'")
                                       == F.to_date(F.lit(data_date), format="yyyy-MM-dd"), F.lit("CDC_INSERT"))
                                .when(F.lower(F.col("isdeleted")) == "true", F.lit("CDC_DELETE"))
                                .when(F.to_date(F.col("systemmodstamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSS'Z'") <
                                      F.to_date(F.lit(data_date), format="yyyy-MM-dd"), F.lit("No Change"))
                                .when(F.to_timestamp(F.col("lastmodifieddate"),
                                                     "yyyy-MM-dd'T'HH:mm:ss.SSSS'Z'") > F.to_timestamp(
                                    F.col("createddate"), "yyyy-MM-dd'T'HH:mm:ss.SSSS'Z'"),
                                      F.lit("CDC_UPDATE"))
                            )
                  .withColumn("ds_cdc_index", F.lit(1).cast(LongType()))
                )

        else:
            # If table doesnt exist then insert all records
            df = (df.withColumn(DsColumn.RECORD_CHANGE_TYPE, F.lit("CDC_INSERT"))
                    .withColumn("ds_cdc_index", F.lit(1).cast(LongType())))

        df1 = (df.where((F.col(DsColumn.RECORD_CHANGE_TYPE) == "CDC_INSERT") & (F.lower(F.col("isdeleted")) == "true"))
                .withColumn(DsColumn.RECORD_CHANGE_TYPE,F.lit("CDC_DELETE"))
                .withColumn("ds_cdc_index", F.lit(2).cast(LongType()))
               )
        final_df = df.union(df1)

        # filter only needed records
        final_df = final_df.where(F.col(DsColumn.RECORD_CHANGE_TYPE).isin("CDC_UPDATE", "CDC_INSERT", "CDC_DELETE"))
        #data_date1 = data_date.strftime("%Y%m%d")
        final_df = (final_df.withColumn("ds_record_source", F.lit(trf.table_name))
            #.withColumn("ds_cdc_index", F.row_number().over(Window.orderBy(trf.primary_key)))
            .withColumn("ds_record_created_timestamp", F.lit(partition_date))
          )

        return final_df


def json_string_to_map_date(df, trf):
    """
    convert columns json string to map(string, date)
    """

    trf = DotDict(trf)
    for column_name in trf.column_patterns:
        df = df.withColumn(column_name, F.from_json(F.col(column_name), MapType(StringType(), DateType())))
    return df


def cast_multiple_fmt_to_timestamp(df, trf):
    trf = DotDict(trf)
    err_msg = "Only `string` types are supported"
    col_types = dict(df.dtypes)
    timestamp_format_list_default = ["yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.SSSSSS", "yyyy-MM-dd HH:mm:ss.SSS"]
    for col in get_columns_from_column_patterns(df, trf.column_patterns):
        col_type = col_types[col]
        if col_type == "string":
            print(timestamp_format_list_default)
            print(trf.timestamp_format_list, type(trf.timestamp_format_list))
            df = df.withColumn(
                col,
                F.coalesce(*[F.to_timestamp(df[col], format=fmt).alias(col) for fmt in trf.timestamp_format_list
                             + timestamp_format_list_default]),
            )
        else:
            raise BaseException(err_msg)
    return df


def maps_to_columns(df: DataFrame, trf):
    from pyspark.sql import functions as F
    trf = DotDict(trf)
    map_columns = trf["map_columns"]
    other_columns = [column for column in df.columns if column not in map_columns]
    df = (
        df.withColumn("_map_concat", F.map_concat(*map_columns))
        .drop(*map_columns)
        .select(*other_columns, F.explode("_map_concat"))
        .groupBy(other_columns).pivot("key").agg(F.first("value"))
    )
    return df