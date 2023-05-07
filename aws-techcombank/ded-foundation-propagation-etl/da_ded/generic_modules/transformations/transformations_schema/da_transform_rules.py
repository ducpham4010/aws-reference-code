"""
Ref:
https://confluence.techcombank.com.vn/display/DataEngineering/Practical+%3A+Rules+and+conventions#Practical:Rulesandconventions-4.TransformRulesforeachfields(version2-callbyRuleCode~TEXT_NON_UNICODE,TEXT_WITH_UNICODE...)
"""
import pytz

from marshmallow import fields, validate

from da_ded.generic_modules.transformations.transformations_schema import Transformation


class da_text_non_unicode(Transformation):
    column_patterns = fields.List(fields.String(), required=True, validate=validate.Length(min=1))


class da_text_with_unicode(Transformation):
    column_patterns = fields.List(fields.String(), required=True, validate=validate.Length(min=1))


class da_date(Transformation):
    column_patterns = fields.List(fields.String(), required=True, validate=validate.Length(min=1))
    date_format = fields.String(load_default=None)


class da_date_time(Transformation):
    column_patterns = fields.List(fields.String(), required=True, validate=validate.Length(min=1))
    timezone = fields.String(load_default="Etc/GMT-7", validate=validate.OneOf(pytz.all_timezones))
    timestamp_format = fields.String(load_default=None)


class da_get_snapshot_from_scd_type_2(Transformation):
    """Get snapshot from a SCD type 2 dataframe. The input dataframe must have correct technique fields.
    Noted:
    - this function assume dataframe is read from "{scd table path}/DsColumn.RECORD_STATUS=1" (only read active record),
    and does not perform active record filter
    - this function only deal with technique fields.
    Rename column, select column, ... if needed should be done elsewhere
    Ref:
    https://confluence.techcombank.com.vn/display/DataEngineering/Transformation+Rules#TransformationRules-2.TransformRulesforSCD
    """

    pass