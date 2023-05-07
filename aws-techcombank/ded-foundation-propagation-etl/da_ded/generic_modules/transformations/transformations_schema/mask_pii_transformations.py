from marshmallow import fields

from da_ded.generic_modules.transformations.transformations_schema import Transformation


class mask_pii(Transformation):
    column_patterns = fields.List(fields.String(), required=True)
    secret_name = fields.String(required=True)


class mask_pii_with_condition(Transformation):
    column_patterns = fields.List(fields.String(), required=True)
    secret_name = fields.String(required=True)
    filter_conditions = fields.Dict(load_default=None)


class fpe_mask_phone_number(Transformation):
    column_patterns = fields.List(fields.String(), required=True)
    secret_name = fields.String(required=True)


class fpe_mask_email(Transformation):
    column_patterns = fields.List(fields.String(), required=True)
    secret_name = fields.String(required=True)


class fpe_mask_datetime(Transformation):
    column_patterns = fields.List(fields.String(), required=True)
    secret_name = fields.String(required=True)
    datetime_format = fields.String(required=True)