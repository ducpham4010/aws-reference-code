""" Spark specific constants """
from enum import Enum

from pyspark.sql.types import DecimalType

DS1_DECIMAL = DecimalType(21, 6)  # common decimal format used for D-S1 data
# more info: https://confluence.techcombank.com.vn/display/DataEngineering/Transform+Rules+-+Overall
SPARK_TIMEZONE_CFG = "spark.sql.session.timeZone"


class SparkSQLDataType(str, Enum):
    STRING = "string"
    INTEGER = "int"
    BIG_INTEGER = "bigint"
    DATE = "date"
    TIMESTAMP = "timestamp"
    DECIMAL = "decimal"

    MAP_STRING_STRING = "map<string,string>"
    MAP_STRING_INTEGER = "map<string,int>"
    MAP_STRING_BIG_INTEGER = "map<string,bigint>"
    MAP_STRING_DATE = "map<string,date>"
    MAP_STRING_TIMESTAMP = "map<string,timestamp>"
    MAP_STRING_DECIMAL_PREFIX = "map<string,decimal"

    ARRAY_STRING = "array<string>"