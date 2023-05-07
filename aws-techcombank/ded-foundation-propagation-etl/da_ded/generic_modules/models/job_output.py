from copy import deepcopy

from marshmallow import Schema, fields, validate, validates_schema, ValidationError
from marshmallow.utils import INCLUDE

from .serde import Serde
from ..models.constants import WriterType, Table, WriteMode


class JobOutputSchema(Schema):
    input_name = fields.String(required=False, load_default=None)
    input_names = fields.List(fields.String, required=False, load_default=None)
    targets = fields.List(fields.Raw())
    transformations = fields.List(fields.Raw(), required=False, load_default=[])
    validations = fields.List(fields.Dict(), required=False, load_default=list)
    schema = fields.Dict(load_default=None)
    migration_type = fields.String(required=False, load_default=None)

    @validates_schema(skip_on_field_errors=True)
    def validate(self, data, **kwargs):
        if not data["input_name"] and not data["input_names"]:
            raise ValidationError("either input_name or input_names must be provided")


class OutputTargetSchema(Schema):
    class Meta:
        unknown = INCLUDE

    # Only writer_type should be required,
    # probably move rest of parameters to separate validation classes of each writer_type
    writer_type = fields.String(required=True, validate=validate.OneOf(WriterType.items()))
    writer_config = fields.Dict(required=False)

    table_type = fields.String(
        required=False,
        load_default=None,
        validate=validate.OneOf(Table.items()),
        allow_none=True,
    )
    path = fields.String(required=False, load_default=None, allow_none=True)
    serde = fields.Dict(required=False, load_default=None, allow_none=True)
    is_diff = fields.Boolean(required=False, load_default=False)
    diff_keys = fields.List(
        required=False,
        load_default=None,
        cls_or_instance=fields.String(),
        allow_none=True,
    )
    is_unique = fields.Boolean(required=False, load_default=False, allow_none=True)
    unique_keys = fields.List(
        required=False,
        load_default=None,
        cls_or_instance=fields.String(),
        allow_none=True,
    )
    partition_by = fields.String(required=False, load_default=None, allow_none=True)
    partition_columns = fields.List(
        required=False,
        load_default=None,
        cls_or_instance=fields.String(),
        allow_none=True,
    )
    write_mode = fields.String(
        required=False,
        load_default=WriteMode.APPEND_ONLY,
        validate=validate.OneOf(WriteMode.items()),
        allow_none=True,
    )
    writer_sub_type = fields.String(required=False, load_default=None, allow_none=True)
    primary_column = fields.String(required=False, load_default=None, allow_none=True)
    key_sm = fields.String(load_default="")
    table = fields.String(load_default="")
    tmp_path = fields.String(required=False, load_default=None)
    golden_path = fields.String(required=False, load_default=None)
    

    @validates_schema(skip_on_field_errors=True)
    def validate(self, data, **kwargs):
        # SONAR: do not merge nested if statements here, more condition might be added later
        if data["writer_type"] == WriterType.T24_TYPE4_CURRENT_FROM_STAGING:
            if not data["tmp_path"]:  # NOSONAR
                raise ValidationError(f"tmp_path is required if writer_type = {data['writer_type']}")

        if data["writer_type"] in (
            WriterType.T24_TYPE2,
            WriterType.T24_TYPE2_COB,
            WriterType.T24_TYPE2_CDC_TDS,
            WriterType.T24_TYPE2_CDC_TDS_COB,
            WriterType.T24_TYPE2_DWH_TDS,
            WriterType.T24_TYPE2_DWH_TDS_COB,
            WriterType.T24_TYPE2_DWH_SNP,
            WriterType.T24_TYPE2_DWH_SNP_COB,
            WriterType.T24_TYPE2_MIGRATION3B,
            WriterType.T24_TYPE2_MIGRATION3B_COB,
            WriterType.T24_TYPE4_CURRENT_FROM_STAGING,
        ):
            if not data["primary_column"]:
                raise ValidationError(f"primary_column is required if writer_type = '{data['writer_type']}'")
            if not data["path"]:
                raise ValidationError(f"path is required if writer_type = '{data['writer_type']}'")

        if data["writer_type"] in (
            WriterType.T24_TYPE4_HIST,
            WriterType.T24_TYPE2_MIGRATION4C_SNP,
            WriterType.T24_TYPE2_MIGRATION4C_SNP_COB,
        ):
            if not data["path"]:  # NOSONAR
                raise ValidationError(f"path is required if writer_type = '{data['writer_type']}'")

        if data["writer_type"] == WriterType.T24_TYPE2_MIGRATION4C_SNP_BACKDATE_COB:
            if not data["golden_path"]:  # NOSONAR
                raise ValidationError(f"golden_path is required if writer_type = '{data['writer_type']}'")

class JobOutput:
    def __init__(self, job_output):
        job_output = JobOutputSchema().load(job_output)
        self.input_name = job_output["input_name"]
        self.input_names = job_output["input_names"]
        self.targets = [OutputTarget(v) for v in job_output["targets"]]
        self.transformations = job_output["transformations"]
        self.validations = job_output["validations"]
        self.schema = job_output["schema"]
        self.migration_type = job_output["migration_type"]

class SForceApiJobOutputSchema(Schema):
    input_name = fields.String()
    output_path = fields.String()
    table_name = fields.String()
    target_type = fields.String(required=False)

class SForceApiJobOutput:
    def __init__(self, job_output) -> None:
        job_output = SForceApiJobOutputSchema().load(job_output)
        self.input_name = job_output["input_name"]
        self.output_path = job_output["output_path"]
        self.table_name = job_output["table_name"]
        self.target_type = job_output["target_type"]



class OutputTarget:
    def __init__(self, output_target):
        # Writers can further additional parameter using this attribute
        self._output_target_dict = deepcopy(output_target)

        output_target = OutputTargetSchema().load(output_target)

        self.writer_type = output_target["writer_type"]
        self.writer_config = output_target.get("writer_config", {})

        self.table_type = output_target["table_type"]
        self.path = output_target["path"]
        self.serde = Serde.create(output_target["serde"])
        self.is_diff = output_target["is_diff"]
        self.diff_keys = output_target["diff_keys"]
        self.is_unique = output_target["is_unique"]
        self.unique_keys = output_target["unique_keys"]
        self.partition_by = output_target["partition_by"]
        self.partition_columns = output_target["partition_columns"]
        self.write_mode = output_target["write_mode"]
        self.writer_sub_type = output_target["writer_sub_type"]
        self.primary_column = output_target["primary_column"]
        self.key_sm = output_target["key_sm"]
        self.table = output_target["table"]
        self.tmp_path = output_target["tmp_path"]
        self.golden_path = output_target["golden_path"]

    @property
    def output_target_dict(self):
        return self._output_target_dict