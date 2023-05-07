from marshmallow import Schema, fields, validates_schema, validate, ValidationError

from .serde import Serde
from .constants import ReaderType, JdbcSupportedConnectionTypes


class JdbcConfig(Schema):
    connection_type = fields.String(required=True, validate=validate.OneOf(JdbcSupportedConnectionTypes.items()))
    aws_secret_identifier = fields.String(required=True)
    table_name = fields.String(required=True)

class DynamoDataLineage(Schema):
    table_name = fields.String(required=True)
    tmp_path = fields.String(required=True)
    tree_lineage_path = fields.String(required=True)
    json_lineage_path = fields.String(required=True)
    lineage_write_mode = fields.String(required=True)
    lineage_file_format = fields.String(required=True)
    

class SForceApiConfig(Schema):
    table_name = fields.String()
    condition = fields.String()
    content_type = fields.String(default="CSV")
    line_ending = fields.String()
    column_delimiter = fields.String()
    aws_secret_name = fields.String()

class CDCTypeConfig(Schema):
    cdc_type = fields.String()
    cdc_fields = fields.List(fields.String(require=False), require=False, allow_none=True)
    checkpoint_time_delta = fields.String(required=False)



class GlueCatalogAdditionalConfig(Schema):
    inferSchema = fields.String(required=False, load_default=None)


class GlueCatalog(Schema):
    database = fields.String(required=True)
    table = fields.String(required=True)
    push_down_predicate = fields.String(required=False, load_default="")
    transformation_ctx = fields.String(required=False, load_default="")
    enable_push_down_predicate = fields.Boolean(required=False, load_default=False)
    partition_column = fields.String(required=False, load_default="ds_partition_date")
    expect_param = fields.String(required=False, load_default="data_date")
    empty_allow = fields.Boolean(required=False, load_default=False)
    fallback = fields.Dict(required=False, load_default=None)
    extend_range = fields.Integer(required=False, load_default=0)
    additional_options = fields.Nested(GlueCatalogAdditionalConfig, required=False, allow_none=True, missing={})


class JobInputSchema(Schema):
    name = fields.String(required=True)
    reader_type = fields.String(
        required=False,
        load_default=ReaderType.RAW,
        validate=validate.OneOf(ReaderType.items()),
    )
    entry_file = fields.String(load_default=None)
    uri_list = fields.List(fields.String(), load_default=None)
    serde = fields.Dict(load_default=None)
    schema = fields.List(fields.Raw(), load_default=None)
    partition_by = fields.String(required=False, load_default=None, allow_none=True)
    glue_catalog = fields.Nested(GlueCatalog, required=False, load_default=None, allow_none=True)
    jdbc_config = fields.Nested(JdbcConfig, load_default=None)
    reader_config = fields.Dict(load_default=None)
    query = fields.String(load_default=None)
    dynamo_datalineage = fields.Nested(DynamoDataLineage, load_default=None, allow_none=True)

    @validates_schema(skip_on_field_errors=True)
    def validate(self, data, **kwargs):
        if data["entry_file"] is not None and data["uri_list"] is not None:
            raise ValidationError("only entry_file or uri_list should be defined in input config")

        if data["reader_type"] == ReaderType.T24_STAGING and not data["glue_catalog"]:
            raise ValidationError(f"glue_catalog is required if reader_type = '{ReaderType.T24_STAGING}'")

        if data["reader_type"] == ReaderType.JDBC and not data["jdbc_config"]:
            raise ValidationError(f"jdbc_config is required if reader_type = '{ReaderType.JDBC}'")

        if data["reader_type"] == ReaderType.CATALOG_SQL and not data["query"]:
            raise ValidationError(f"query is required if reader_type = '{ReaderType.CATALOG_SQL}'")


class SForceApiInputSchema(Schema):
    name = fields.String(required=True)
    reader_type = fields.String(
        required=False,
        load_default=ReaderType.SFORCE_API,
        validate=validate.OneOf(ReaderType.items()),
    )
    sforce_api_config = fields.Nested(SForceApiConfig, load_default=None)
    cdc_config = fields.Nested(CDCTypeConfig, load_default=None)


class JobInput:
    def __init__(self, job_input: dict):
        job_input = JobInputSchema().load(job_input)
        self.name = job_input["name"]
        self.entry_file = job_input["entry_file"]
        self.uri_list = job_input["uri_list"]
        self.serde = Serde.create(job_input["serde"])
        self.schema = job_input["schema"]
        self.partition_by = job_input["partition_by"]
        self.reader_type = job_input["reader_type"]
        self.glue_catalog = job_input["glue_catalog"]
        self.jdbc_config = job_input["jdbc_config"]
        self.reader_config = job_input["reader_config"]
        self.query = job_input["query"]
        self.dynamo_datalineage = job_input["dynamo_datalineage"]


class SForceApiJobInput:
    def __init__(self, job_input: dict):
        job_input = SForceApiInputSchema().load(job_input)
        self.name = job_input["name"]
        self.reader_type = job_input["reader_type"]
        self.sforce_api_config = dict(job_input["sforce_api_config"])
        self.cdc_config =  dict(job_input["cdc_config"])