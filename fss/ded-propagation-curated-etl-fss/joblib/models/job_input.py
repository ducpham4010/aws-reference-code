from pyspark.sql.session import SparkSession
from marshmallow import Schema, fields
from jeda.models.serde import Serde
from jeda.models.job_input import JobInput,JobInputSchema

class CuratedJobInputSchema(JobInputSchema):
    query = fields.String(load_default=None)

class CuratedJobInput:
    def __init__(self, job_input: dict):
        job_input = CuratedJobInputSchema().load(job_input)
        self.name = job_input.get('name')
        self.entry_file = job_input.get('entry_file')
        self.uri_list = job_input.get('uri_list')
        self.serde = Serde.create(job_input.get('serde'))
        self.schema = job_input.get('schema')
        self.partition_by = job_input.get('partition_by')
        self.reader_type = job_input.get('reader_type')
        self.glue_catalog = job_input.get('glue_catalog')
        self.jdbc_config = job_input.get('jdbc_config')
        self.query = job_input.get('query')