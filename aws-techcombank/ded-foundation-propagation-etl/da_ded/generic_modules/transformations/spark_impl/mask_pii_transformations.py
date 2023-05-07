import pyspark.sql.functions as F
from pyspark.sql.functions import col as col_df, when
from pyspark.sql.types import (
    DateType,
    StringType,
    MapType,
    ArrayType,
    IntegerType,
    LongType,
    DecimalType,
    TimestampType,
)

from da_ded.generic_modules.models.base_models import DotDict
from da_ded.utils import get_secret, parse_decimal_column_type
from .constants import SparkSQLDataType
from .de_transformations import _transform_dict_value, _transform_list_value, _transform_single_value
from .mask_pii_function import (
    mask_word,
    mask_phone_number,
    create_mask_word_function,
    mask_email,
    create_mask_datetime_function,
)
from .utils import get_columns_from_column_patterns


def create_mask_pii_df(mask_f):
    def mask_pii_df(df, trf):
        trf = DotDict(trf)
        col_types = dict(df.dtypes)
        secret_name = trf.secret_name
        key = list(get_secret(secret_name).values())[0]
        _mask_value = create_mask_word_function(key, mask_f)
        for col in get_columns_from_column_patterns(df, trf.column_patterns):
            col_type = col_types[col]
            if col_type == SparkSQLDataType.STRING:
                spark_udf = F.udf(_mask_value, returnType=StringType())
                df = df.withColumn(col, spark_udf(col_df(col)))
            elif col_type == SparkSQLDataType.INTEGER:
                spark_udf = F.udf(_transform_single_value(_mask_value, datatype=SparkSQLDataType.INTEGER),
                                  returnType=IntegerType())
                df = df.withColumn(col, spark_udf(col_df(col)))
            elif col_type == SparkSQLDataType.BIG_INTEGER:
                spark_udf = F.udf(_transform_single_value(_mask_value, datatype=SparkSQLDataType.INTEGER),
                                  returnType=LongType())
                df = df.withColumn(col, spark_udf(col_df(col)))
            elif col_type.startswith(SparkSQLDataType.DECIMAL):
                integer, fractional = parse_decimal_column_type(col_type)
                spark_udf = F.udf(_transform_single_value(_mask_value, datatype=SparkSQLDataType.DECIMAL,
                                                          kwargs={"fractional": fractional}),
                                  returnType=DecimalType(integer, fractional))
                df = df.withColumn(col, spark_udf(col_df(col)))

            elif col_type == SparkSQLDataType.MAP_STRING_STRING:
                spark_udf = F.udf(_transform_dict_value(_mask_value, datatype=SparkSQLDataType.STRING),
                                  returnType=MapType(StringType(), StringType()))
                df = df.withColumn(col, spark_udf(col_df(col)))
            elif col_type == SparkSQLDataType.MAP_STRING_INTEGER:
                spark_udf = F.udf(_transform_dict_value(_mask_value, datatype=SparkSQLDataType.INTEGER),
                                  returnType=MapType(StringType(), LongType()))
                df = df.withColumn(col, spark_udf(col_df(col)))
            elif col_type == SparkSQLDataType.MAP_STRING_BIG_INTEGER:
                spark_udf = F.udf(_transform_dict_value(_mask_value, datatype=SparkSQLDataType.BIG_INTEGER),
                                  returnType=MapType(StringType(), LongType()))
                df = df.withColumn(col, spark_udf(col_df(col)))
            elif col_type.startswith(SparkSQLDataType.MAP_STRING_DECIMAL_PREFIX):
                integer, fractional = parse_decimal_column_type(col_type)
                spark_udf = F.udf(
                    _transform_dict_value(_mask_value, datatype=SparkSQLDataType.DECIMAL,
                                          kwargs={"fractional": fractional}),
                    returnType=MapType(StringType(), DecimalType(integer, fractional))
                )
                df = df.withColumn(col, spark_udf(col_df(col)))

            elif col_type == SparkSQLDataType.ARRAY_STRING:
                spark_udf = F.udf(_transform_list_value(_mask_value), returnType=ArrayType(StringType()))
                df = df.withColumn(col, spark_udf(col_df(col)))
            else:
                continue
        return df

    return mask_pii_df


def create_mask_pii_datetime_df(mask_datetime_f):
    def mask_pii_df(df, trf):
        trf = DotDict(trf)
        col_types = dict(df.dtypes)
        secret_name = trf.secret_name
        key = list(get_secret(secret_name).values())[0]
        datetime_format = trf.datetime_format
        mask_f = mask_datetime_f(datetime_format)
        _mask_value = create_mask_word_function(key, mask_f)
        for col in get_columns_from_column_patterns(df, trf.column_patterns):
            col_type = col_types[col]
            if col_type == SparkSQLDataType.STRING:
                spark_udf = F.udf(_mask_value, returnType=StringType())
                df = df.withColumn(col, spark_udf(col_df(col)))
            elif col_type == SparkSQLDataType.DATE:
                spark_udf = F.udf(_transform_single_value(_mask_value, datatype=SparkSQLDataType.DATE,
                                                          kwargs={"datetime_format": datetime_format}),
                                  returnType=DateType())
                df = df.withColumn(col, spark_udf(col_df(col)))
            elif col_type == SparkSQLDataType.TIMESTAMP:
                spark_udf = F.udf(_transform_single_value(_mask_value, datatype=SparkSQLDataType.TIMESTAMP,
                                                          kwargs={"datetime_format": datetime_format}),
                                  returnType=TimestampType())
                df = df.withColumn(col, spark_udf(col_df(col)))
            elif col_type == SparkSQLDataType.INTEGER:
                spark_udf = F.udf(_transform_single_value(_mask_value, datatype=SparkSQLDataType.INTEGER),
                                  returnType=IntegerType())
                df = df.withColumn(col, spark_udf(col_df(col)))
            elif col_type == SparkSQLDataType.BIG_INTEGER:
                spark_udf = F.udf(_transform_single_value(_mask_value, datatype=SparkSQLDataType.INTEGER),
                                  returnType=LongType())
                df = df.withColumn(col, spark_udf(col_df(col)))

            elif col_type == SparkSQLDataType.MAP_STRING_STRING:
                spark_udf = F.udf(_transform_dict_value(_mask_value, datatype=SparkSQLDataType.STRING),
                                  returnType=MapType(StringType(), StringType()))
                df = df.withColumn(col, spark_udf(col_df(col)))
            elif col_type == SparkSQLDataType.MAP_STRING_DATE:
                spark_udf = F.udf(_transform_dict_value(_mask_value, datatype=SparkSQLDataType.DATE,
                                                        kwargs={"datetime_format": datetime_format}),
                                  returnType=MapType(StringType(), DateType()))
                df = df.withColumn(col, spark_udf(col_df(col)))
            elif col_type == SparkSQLDataType.MAP_STRING_INTEGER:
                spark_udf = F.udf(_transform_dict_value(_mask_value, datatype=SparkSQLDataType.INTEGER),
                                  returnType=MapType(StringType(), LongType()))
                df = df.withColumn(col, spark_udf(col_df(col)))
            elif col_type == SparkSQLDataType.MAP_STRING_BIG_INTEGER:
                spark_udf = F.udf(_transform_dict_value(_mask_value, datatype=SparkSQLDataType.BIG_INTEGER),
                                  returnType=MapType(StringType(), LongType()))
                df = df.withColumn(col, spark_udf(col_df(col)))

            elif col_type == SparkSQLDataType.ARRAY_STRING:
                spark_udf = F.udf(_transform_list_value(_mask_value), returnType=ArrayType(StringType()))
                df = df.withColumn(col, spark_udf(col_df(col)))
            else:
                continue
        return df

    return mask_pii_df


def create_mask_pii_df_with_condition(mask_f):
    def mask_pii_df(df, trf):
        trf = DotDict(trf)
        col_types = dict(df.dtypes)
        secret_name = trf.secret_name
        key = list(get_secret(secret_name).values())[0]
        _mask_value = create_mask_word_function(key, mask_f)

        for col in get_columns_from_column_patterns(df, trf.column_patterns):
            col_type = col_types[col]
            if col_type == SparkSQLDataType.STRING:
                spark_udf = F.udf(_mask_value, returnType=StringType())
                if "not_equal" in trf.filter_conditions:
                    src_column = trf["filter_conditions"]["not_equal"]["source_column"]
                    dst_column = trf["filter_conditions"]["not_equal"]["destination_column"]
                    df = df.withColumn(
                        col,
                        when((col_df(src_column) != col_df(dst_column)), spark_udf(col_df(src_column))).otherwise(
                            col_df(dst_column)
                        ),
                    )
                else:
                    # TODO: Redesign / Handle more conditions
                    continue
            else:
                # TODO: Handle more column types
                continue
            return df

    return mask_pii_df


mask_pii = create_mask_pii_df(mask_word)
mask_pii_with_condition = create_mask_pii_df_with_condition(mask_word)
fpe_mask_phone_number = create_mask_pii_df(mask_phone_number)
fpe_mask_email = create_mask_pii_df(mask_email)
fpe_mask_datetime = create_mask_pii_datetime_df(create_mask_datetime_function)