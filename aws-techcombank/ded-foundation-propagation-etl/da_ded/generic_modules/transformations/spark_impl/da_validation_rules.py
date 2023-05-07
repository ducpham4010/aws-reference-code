import re
from functools import reduce

from marshmallow import Schema, fields, ValidationError
from pyspark.sql import DataFrame
from pyspark.sql import functions as psf
from pyspark.sql.types import ArrayType, StringType

from . import get_col_type, get_columns_from_column_patterns
from ..transformations_schema.da_validation_rules import (
    MandatoryInput,
    ValidCustomerId,
    ValidNumeric,
    ValidPhoneNumber,
    ValidEmail,
    ValidMnemonic,
    RemoveDuplicateCustomerId,
    ValidAlphaNumeric,
    ValidNumericString,
)
from ...models.base_models import DotDict

__registry = {}


def register_validation(name: str, schema):
    def decorator(func):
        __registry[name] = {"func": func, "schema": schema}
        return func

    return decorator


def run_validation(validation: dict, df: DataFrame) -> DataFrame:
    name = validation.get("name")

    if name is None:
        raise RuntimeError("Validation does not have `name`")

    if name not in __registry:
        raise RuntimeError(f"Validation {name} not found")

    validation_func = __registry[name]["func"]
    validation_schema = __registry[name]["schema"]
    validation = DotDict(validation_schema().load(validation))

    return validation_func(df, validation)


def _mandatory_filter(df: DataFrame, col: str) -> DataFrame:
    condition = psf.col(col).isNotNull()

    if get_col_type(df, col) == "string":
        condition = condition & (psf.trim(psf.col(col)) != "")

    return df.filter(condition)


@register_validation("mandatory_input", MandatoryInput)
def mandatory_input(df: DataFrame, validation: DotDict) -> DataFrame:
    for col in get_columns_from_column_patterns(df, validation.column_patterns):
        df = _mandatory_filter(df, col)

    return df


@register_validation("valid_customer_id", ValidCustomerId)
def validate_customer_id(df: DataFrame, validation: DotDict) -> DataFrame:
    for col in get_columns_from_column_patterns(df, validation.column_patterns):
        df = _mandatory_filter(df, col)
        df = _validate_numeric(df, col, is_mandatory=True, min_value=0, max_value=9999999999)

    return df


@register_validation("remove_duplicate_customer_id", RemoveDuplicateCustomerId)
def remove_duplicate_customer_id(df: DataFrame, validation: DotDict) -> DataFrame:
    column_id = get_columns_from_column_patterns(df, validation.column_patterns).pop()
    df_id = df.select(column_id).groupBy(column_id).agg(psf.count(column_id).alias("count_number"))

    unique_id_df = df_id.filter("count_number = 1").select(column_id).collect()
    unique_id = [c[column_id] for c in unique_id_df]
    df = df.filter(psf.col(column_id).isin(unique_id))
    return df


def _validate_numeric(df: DataFrame, col: str, is_mandatory: bool, min_value=None, max_value=None) -> DataFrame:
    min_condition, max_condition = None, None
    if min_value is not None:
        min_condition = psf.col(col) >= min_value
    if max_value is not None:
        max_condition = psf.col(col) <= max_value

    condition = reduce(
        lambda c1, c2: c1 & c2,
        filter(lambda c: c is not None, [min_condition, max_condition]),
    )

    # Drop the whole row if invalid
    if is_mandatory:
        return df.filter(condition)

    return df.withColumn(col, psf.when(condition, psf.col(col)).otherwise(None))


@register_validation("valid_numeric", ValidNumeric)
def validate_numeric(df: DataFrame, validation: DotDict) -> DataFrame:
    for col in get_columns_from_column_patterns(df, validation.column_patterns):
        df = _validate_numeric(df, col, validation.is_mandatory, validation.min_value, validation.max_value)

    return df


def _validate_alphanumeric(df: DataFrame, col: str, is_mandatory: bool) -> DataFrame:
    """
    start string, any character in a-z or A-Z or 0-9, matched one or more times, end string.
    """
    condition = psf.col(col).rlike("^[A-Za-z0-9]{1,}$")
    if is_mandatory:
        return df.filter(condition)
    return df.withColumn(col, psf.when(condition, psf.col(col)).otherwise(None))


@register_validation("valid_alphanumeric", ValidAlphaNumeric)
def validate_alphanumeric(df: DataFrame, validation: DotDict) -> DataFrame:
    for col in get_columns_from_column_patterns(df, validation.column_patterns):
        df = _validate_alphanumeric(df, col, validation.is_mandatory)
    return df


def _validate_numeric_string(df: DataFrame, col: str, is_mandatory: bool) -> DataFrame:
    """
    start string, any character 0-9, matched one or more times, end string.
    """
    condition = psf.col(col).rlike("^[0-9]{1,}$")
    if is_mandatory:
        return df.filter(condition)
    return df.withColumn(col, psf.when(condition, psf.col(col)).otherwise(None))


@register_validation("valid_numeric_string", ValidNumericString)
def validate_digits(df: DataFrame, validation: DotDict) -> DataFrame:
    for col in get_columns_from_column_patterns(df, validation.column_patterns):
        df = _validate_numeric_string(df, col, validation.is_mandatory)
    return df


def _validate_phone_number_list(phone_num_list):
    if phone_num_list is None:
        return None

    result = []
    pattern = r"[0-9+(). ]+"

    for phone_number in phone_num_list:
        if phone_number is None or not re.fullmatch(pattern, phone_number):
            result.append(None)
            continue

        result.append(re.sub(r"[+(). ]+", "", phone_number))

    return result


@register_validation("valid_phone_number", ValidPhoneNumber)
def validate_phone_number(df: DataFrame, validation: DotDict) -> DataFrame:
    col_types = dict(df.dtypes)
    spark_udf = psf.udf(_validate_phone_number_list, returnType=ArrayType(StringType()))

    for col in get_columns_from_column_patterns(df, validation.column_patterns):
        col_type = col_types[col]
        assert col_type == "array<string>", "only support array string"
        df = df.withColumn(col, spark_udf(df[col]))

    return df


class _ValidEmail(Schema):
    email = fields.Email(required=True)


def _validate_email(email_list):
    if email_list is None:
        return None

    schema = _ValidEmail()
    result = []

    for email in email_list:
        try:
            schema.load({"email": email})
            result.append(email)
        except ValidationError:
            result.append(None)

    return result


@register_validation("valid_email", ValidEmail)
def validate_email(df: DataFrame, validation: DotDict) -> DataFrame:
    col_types = dict(df.dtypes)
    spark_udf = psf.udf(_validate_email, returnType=ArrayType(StringType()))
    for col in get_columns_from_column_patterns(df, validation.column_patterns):
        col_type = col_types[col]
        assert col_type == "array<string>", "only support array string"
        df = df.withColumn(col, spark_udf(df[col]))

    return df


def _validate_mnemonic(value):
    if value is None:
        return None

    if len(value) < 3 or len(value) > 10:
        return None

    regex = r"^[A-Z]+[A-Z0-9.]+$"
    if re.match(regex, value):
        return value

    return None


@register_validation("valid_mnemonic", ValidMnemonic)
def validate_mnemonic(df: DataFrame, validation: DotDict) -> DataFrame:
    col_types = dict(df.dtypes)
    spark_udf = psf.udf(_validate_mnemonic, returnType=StringType())

    for col in get_columns_from_column_patterns(df, validation.column_patterns):
        col_type = col_types[col]
        assert col_type == "string", "only support string"
        df = df.withColumn(col, spark_udf(df[col]))
        df = _mandatory_filter(df, col)

    return df