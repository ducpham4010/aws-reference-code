import logging
import math
from copy import deepcopy
from datetime import datetime, timedelta, date

import boto3
import pyspark.sql.functions as F
from dateutil.relativedelta import relativedelta
from jeda.models.job_output import OutputTarget, WriteMode
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StringType, StructField, StructType, MapType

from joblib.common.error import CEGeneralException
from joblib.common.error import InputEmptyException
from joblib.constant.common_values import DEFAULT_PARTITION_RECORD
from joblib.writers.iceberg import scd4current
from ..constant import common_values as CMV
from ..constant.constants import TechnicalColumn
from ..models.job_input import CuratedJobInput
from ..readers.catalog_sql import CatalogSQLReader

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

DATE_FORMAT = "%Y-%m-%d"


def load_input_to_df(job_input: CuratedJobInput, spark, glue_context):
    from jeda.readers.factory import create_reader

    if job_input.reader_type == "catalog_sql":
        reader = CatalogSQLReader(job_input, spark, glue_context)
    else:
        reader = create_reader(job_input, spark, glue_context, None)
    return reader.read()


def read_dfs_by_inputs(spark, glue_context, job_config):
    dfs = {}
    for each_job_input in job_config.input:
        dfs[each_job_input.name] = load_input_to_df(each_job_input, spark, glue_context)
    return dfs


def check_df_input(df, glue_context, cols_select, is_main=False):
    if df is None and is_main:
        raise InputEmptyException()
    count = df.count()
    if is_main:
        if count == 0:
            raise InputEmptyException()
    else:
        if count == 0:
            logger.warning("Sub table don't have data")
            schema = StructType(
                [
                    StructField(column_name, StringType(), True)
                    for column_name in cols_select
                ]
            )

            # create empty dataframe within the above schema
            df_sub = glue_context.createDataFrame([], schema)
            logger.warning("Created empty schema")
            return df_sub
    return df


def optimize_write_partition(df: DataFrame):
    current_num_partitions = df.rdd.getNumPartitions()
    result_count = df.count()
    new_num_partitions = math.ceil(result_count / DEFAULT_PARTITION_RECORD)
    new_num_partitions = new_num_partitions if new_num_partitions > 0 else 1
    if new_num_partitions > current_num_partitions:
        df = df.repartition(new_num_partitions)
    else:
        df = df.coalesce(new_num_partitions)
    return df


def write_to_s3_with_partition_columns(
    spark: SparkSession, df: DataFrame, target: OutputTarget, has_header: bool = False
):
    try:
        CONF_DYNAMIC = "spark.sql.sources.partitionOverwriteMode"
        if target.write_mode == WriteMode.OVERWRITE_PARTITION:
            # setting this to overwrite by partition
            current_conf = spark.conf.get(CONF_DYNAMIC)
            spark.conf.set(CONF_DYNAMIC, "dynamic")
            if not target.partition_columns:
                raise CEGeneralException(
                    f"Partition columns must not Empty in mode {target.write_mode}!"
                )
            if has_header:
                df.write.format(target.serde.format).option("header", "true").option(
                    "escape", '"'
                ).partitionBy(*target.partition_columns).mode("overwrite").save(
                    target.path
                )
            else:
                df.write.format(target.serde.format).partitionBy(
                    *target.partition_columns
                ).mode("overwrite").save(target.path)
            spark.conf.set(CONF_DYNAMIC, current_conf)
            return True

        if not target.partition_columns:
            if has_header:
                df.write.format(target.serde.format).option("header", "true").option(
                    "escape", '"'
                ).mode(target.write_mode).save(target.path)
            else:
                df.write.format(target.serde.format).mode(target.write_mode).save(
                    target.path
                )
        else:
            if has_header:
                df.write.format(target.serde.format).option("header", "true").option(
                    "escape", '"'
                ).partitionBy(*target.partition_columns).mode(target.write_mode).save(
                    target.path
                )
            else:
                df.write.format(target.serde.format).partitionBy(
                    *target.partition_columns
                ).mode(target.write_mode).save(target.path)
        return True
    except Exception as e:
        print("Exception: " + str(e))
        return False


def check_if_s3_path_exists(target_path):
    s3 = boto3.client("s3")
    bucket_name = target_path.split("/")[2]
    s3_path = "/".join(target_path.split("/")[3:])
    try:
        s3.head_object(Bucket=bucket_name, Key=s3_path)
        print(f"S3 path {s3_path} exists in bucket {bucket_name}")
        return 1
    except Exception:
        print(f"S3 path {s3_path} does not exist in bucket {bucket_name}")
        return 0


def identify_changed_recs(spark_session, source_df, target_config):
    # Read the data from target path
    config = deepcopy(target_config._output_target_dict)
    target_data_config = dict((k, v) for k, v in config.items() if v is not None)
    print("target_data_config: ", target_data_config)

    target_path = target_data_config["path"]

    if check_if_s3_path_exists(target_path):
        target_df = spark_session.read.parquet(target_path).cache()
    else:
        target_df = source_df.where("1 == 2")

    if target_df.count() > 0:
        temp_location = (
            "/".join(target_path.split("/")[:-1])
            + "/_tmp/"
            + target_path.split("/")[-1]
        )
        target_df.write.mode("overwrite").format("parquet").save(temp_location)

    # Identify changed/new records
    source_df.createOrReplaceTempView("source")
    target_df.createOrReplaceTempView("target")

    # Generate primary_cols_str
    primary_cols = target_data_config["primary_columns"]

    temp = []
    for col_name in primary_cols:
        temp.append(f"src.{col_name} = tgt.{col_name}")

    primary_cols_str = " and ".join(temp)

    print("primary_cols_str:", primary_cols_str)

    # Generate column list for select clause
    technical_columns = [
        TechnicalColumn.INSERTED_DATE,
        TechnicalColumn.UPDATED_DATE,
        TechnicalColumn.RECORD_STATUS,
    ]

    columns_str = ", ".join(
        [
            "src." + col_name
            for col_name in source_df.columns
            if col_name not in technical_columns
        ]
    )

    df_updated_records = spark_session.sql(
        f"""
            select
                {columns_str},
                case 
                    when tgt.{TechnicalColumn.INSERTED_DATE} is null then src.{TechnicalColumn.INSERTED_DATE} 
                    else tgt.{TechnicalColumn.INSERTED_DATE} 
                end as {TechnicalColumn.INSERTED_DATE},
                src.{TechnicalColumn.UPDATED_DATE} as {TechnicalColumn.UPDATED_DATE},
                src.{TechnicalColumn.RECORD_STATUS} as {TechnicalColumn.RECORD_STATUS}
            from
            source src
            inner join target tgt on {primary_cols_str}
        """
    )

    df_new_records = spark_session.sql(
        f"""
            select
                src.*
            from
            source src
            left join target tgt on {primary_cols_str}
            where tgt.{TechnicalColumn.INSERTED_DATE} is null
        """
    )

    df_unchanged_records = spark_session.sql(
        f"""
            select
                tgt.*
            from
            source src
            right join target tgt on {primary_cols_str}
            where src.{TechnicalColumn.INSERTED_DATE} is null            
        """
    )

    final_df = df_updated_records.unionByName(df_new_records).unionByName(
        df_unchanged_records
    )

    return final_df


def get_data_type(df: DataFrame, data_type: str):
    data_types = []
    for entry in df.schema.fields:
        if str(entry.dataType).lower().startswith(data_type.lower()):
            data_types.append(entry.name)
    return data_types


def add_ds_hash_index(
    df: DataFrame,
    excluded_cols=None,
    must_include_cols=None,
    hash_index_col=TechnicalColumn.DS_HASH_INDEX,
):
    """
    add `TechnicalColumn.DS_HASH_INDEX` to given dataframe by using builtin spark.functions.hash.
    Please note that the technical columns (prefixed with 'ds_') will be excluded by default
    """
    if excluded_cols is None:
        excluded_cols = []
    if must_include_cols is None:
        must_include_cols = []

    map_columns = get_data_type(df, data_type="Map")

    if map_columns:
        select_expr = [
            *(F.to_json(F.col(x)).alias("cast_string_" + x) for x in map_columns),
            *df.columns,
        ]
        df = df.select(*select_expr)

    # remove excluded columns and technical fields
    def filter_rules(x):
        return (x in must_include_cols) or (
            (x not in excluded_cols + map_columns) and (not str(x).startswith("dhos_"))
        )

    selected = list(filter(filter_rules, df.columns))
    return df.withColumn(
        hash_index_col, F.sha2(F.concat_ws("||", *selected), 256)
    ).drop(*["cast_string_" + c for c in map_columns])


def check_empty_input_df(df, select_cols, glue_context):
    if df.count() == 0:
        logger.info("-----creating empty dataframe-----")
        empty_schema = StructType(
            [
                StructField(column_name, StringType(), True)
                for column_name in select_cols
            ]
        )
        empty_df = glue_context.createDataFrame([], schema=empty_schema)
        output_df = empty_df
    else:
        output_df = df
    return output_df


def convert_str_to_date(date_string):
    return datetime.strptime(date_string, DATE_FORMAT).date()


def get_datetime_iterator(start_date: str, end_date: str, return_date_type=True):
    """Create a datetime generator to iterate from `start_date` to `end_date`"""
    start_date = convert_str_to_date(start_date)
    end_date = convert_str_to_date(end_date)
    delta = timedelta(days=1)
    while start_date <= end_date:
        if return_date_type:
            yield start_date
        else:
            yield start_date.strftime(DATE_FORMAT)
        start_date = start_date + delta


def get_last_3_month_date_exclusive(data_date: str) -> str:  # pragma: no cover
    date = datetime.strptime(data_date, "%Y-%m-%d")
    date = date - relativedelta(months=3) + relativedelta(days=1)
    return date.strftime("%Y-%m-%d")


def get_date_diff(start_date: str, end_date: str):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    return (end_date - start_date).days


def create_table_from_dataframe(
    spark_session: SparkSession,
    df: DataFrame,
    database_name: str,
    table_name: str,
    partition_column: str,
):
    df.createOrReplaceTempView(f"tmp_{table_name}")

    query = f"""
    CREATE TABLE glue_catalog.{database_name}.{table_name}
    USING iceberg
    PARTITIONED BY ({partition_column})
    AS SELECT * FROM tmp_{table_name}
    """
    print("Execute query:", query)
    spark_session.sql(query)


def json_string_to_array_map_string(df, columns):
    """
    convert columns json string to array[map(string, string)]
    """
    for column_name in columns:
        df = df.withColumn(
            column_name,
            F.array(
                F.from_json(F.col(column_name), MapType(StringType(), StringType()))
            ),
        )
    return df


def identify_changed_records(spark_session, source_df, target_config):
    # Read the data from target path
    config = deepcopy(target_config._output_target_dict)
    target_data_config = dict((k, v) for k, v in config.items() if v is not None)
    print("target_data_config: ", target_data_config)

    catalog_name = target_data_config["catalog_name"]
    tbl_name = target_data_config["tbl_name"]
    db_name = target_data_config["db_name"]

    if is_table_exist(spark_session,target_data_config):
        target_df = spark_session.table(
        "{0}.{1}.{2}".format(catalog_name, db_name, tbl_name)
    )
    else:
        target_df = source_df.where("1 == 2")

    # Identify changed/new records
    source_df.createOrReplaceTempView("source")
    target_df.createOrReplaceTempView("target")

    # Generate primary_cols_str
    primary_cols = target_data_config["primary_columns"]
    temp = []
    for col_name in primary_cols:
        temp.append(f"src.{col_name} = tgt.{col_name}")
    primary_cols_str = " and ".join(temp)

    print("primary_cols_str:", primary_cols_str)

    # Generate column list for select clause
    technical_columns = [
        TechnicalColumn.INSERTED_DATE,
        TechnicalColumn.UPDATED_DATE,
        TechnicalColumn.RECORD_STATUS,
    ]

    columns_str = ", ".join(
        [
            f"nvl(src.{c},tgt.{c}) as {c}"
            for c in source_df.columns
            if c not in technical_columns
        ]
    )
    print(f"select columns {columns_str}")

    final_df = spark_session.sql(
        f"""
            select 
            {columns_str},
            nvl(tgt.{TechnicalColumn.INSERTED_DATE},src.{TechnicalColumn.INSERTED_DATE}) as {TechnicalColumn.INSERTED_DATE},
            nvl(src.{TechnicalColumn.UPDATED_DATE},tgt.{TechnicalColumn.UPDATED_DATE}) as {TechnicalColumn.UPDATED_DATE},
            nvl(src.{TechnicalColumn.RECORD_STATUS},tgt.{TechnicalColumn.RECORD_STATUS}) as {TechnicalColumn.RECORD_STATUS}
        from 
        source as src
        full outer join
        target as tgt on {primary_cols_str}
        """
    )

    return final_df


def is_table_exist(spark_session, table_config):
    try:
        catalog_name = table_config["catalog_name"]
        tbl_name = table_config["tbl_name"]
        db_name = table_config["db_name"]

        return (
            spark_session.table(
                "{0}.{1}.{2}".format(catalog_name, db_name, tbl_name)
            ).count()
            >= 0)
    except Exception as ex:
        print(ex)
        return False
    
def check_table_exist(glue_context, database: str, table: str):
    try:
        return (
                glue_context.spark_session.sql(
                    f"select * from glue_catalog.{database}.{table} limit 10"
                ).count()
                >= 0
        )
    except Exception as ex:
        print(ex)
        return False


def create_table_iceberg_by_dataframe(spark_session, df, table_config):
    catalog_name = table_config["catalog_name"]
    tbl_name = table_config["tbl_name"]
    db_name = table_config["db_name"]
    path = table_config["path"]
    partition_columns = table_config["partition_columns"]
    partition_by_str = f"PARTITIONED BY ({','.join(partition_columns if partition_columns else [])})"
    df.createOrReplaceTempView(f"tmp_{tbl_name}")

    query = f"""
        CREATE TABLE {catalog_name}.{db_name}.{tbl_name}
        USING iceberg
        {partition_by_str if partition_columns else ""}
        LOCATION '{path}'
        AS SELECT * FROM tmp_{tbl_name}
        """
    spark_session.sql(query)
    print(f"Created table not exist completed {catalog_name}.{db_name}.{tbl_name}")


def execute_writer(spark, df: DataFrame, target: OutputTarget, writer):
    config = deepcopy(target.output_target_dict)
    table_config = dict((k, v) for k, v in config.items() if v is not None)

    if is_table_exist(spark, table_config):
        writer.write()
    else:
        create_table_iceberg_by_dataframe(spark, df, table_config)


def ctas_if_not_exist(
    spark_session, catalog, database_name: str, table_name: str,partition_columns: str, schema: str
):
    query = f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{database_name}.{table_name}
    (
        {schema}
    )
    PARTITIONED BY ({partition_columns}) 
    LOCATION 's3://{{golden_bucket}}/golden_curated/{table_name}/'
    TBLPROPERTIES ( 'table_type'='ICEBERG', 'format'='parquet'
    )
    """
    print("Execute query:", query)
    spark_session.sql(query)