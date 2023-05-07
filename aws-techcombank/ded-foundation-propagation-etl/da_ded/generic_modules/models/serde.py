from marshmallow import Schema, fields, validate, EXCLUDE

from .base_models import DotDict
from .constants import SerializationFormats


class SerdeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    format = fields.String(required=True, validate=validate.OneOf(SerializationFormats.items()))


class CsvSerdeSchema(SerdeSchema):
    encoding = fields.String(load_default="utf-8")
    delimiter = fields.String(load_default=",")
    escape = fields.String(load_default='"', allow_none=True)
    quote = fields.String(load_default='"')
    header = fields.Boolean(load_default=True)
    multi_line = fields.Boolean(load_default=False)


class ParquetSerdeSchema(SerdeSchema):
    pass


class JsonSerdeSchema(SerdeSchema):
    multiLine = fields.Boolean(load_default=False)
    dateFormat = fields.String(load_default=None)
    timestampFormat = fields.String(load_default=None)
    encoding = fields.String(load_default=None)
    lineSep = fields.String(load_default=None)
    samplingRatio = fields.Float(load_default=None)
    dropFieldIfAllNull = fields.Boolean(load_default=None)


class Serde:
    @staticmethod
    def create(serde: dict):
        if serde is None:
            return None
        if serde["format"] == SerializationFormats.CSV:
            serde = CsvSerdeSchema().load(serde)
        elif serde["format"] == SerializationFormats.PARQUET:
            serde = ParquetSerdeSchema().load(serde)
        elif serde["format"] == SerializationFormats.JSON:
            serde = JsonSerdeSchema().load(serde)
        return DotDict(serde)