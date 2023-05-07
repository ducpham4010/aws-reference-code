from marshmallow import fields, validate, validates_schema, ValidationError

from . import Transformation


class _ColumnPatternsInput(Transformation):
    column_patterns = fields.List(fields.String(), required=True, validate=validate.Length(min=1))


class MandatoryInput(_ColumnPatternsInput):
    pass


class ValidCustomerId(_ColumnPatternsInput):
    pass


class RemoveDuplicateCustomerId(_ColumnPatternsInput):
    pass


class ValidNumeric(_ColumnPatternsInput):
    min_value = fields.Number(required=False, load_default=None)
    max_value = fields.Number(required=False, load_default=None)
    is_mandatory = fields.Boolean(required=True)

    @validates_schema(skip_on_field_errors=True)
    def validate(self, data, **kwargs):
        if data["min_value"] is None and data["max_value"] is None:
            raise ValidationError("either min_value or max_value is required")


class ValidNumericString(_ColumnPatternsInput):
    is_mandatory = fields.Boolean(required=True)


class ValidAlphaNumeric(_ColumnPatternsInput):
    is_mandatory = fields.Boolean(required=True)


class ValidPhoneNumber(_ColumnPatternsInput):
    pass


class ValidEmail(_ColumnPatternsInput):
    pass


class ValidMnemonic(_ColumnPatternsInput):
    pass