from typing import Union, List, Dict
from copy import deepcopy

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from da_ded.exceptions import BaseException
from .executor import Executor
from ..models.job_config import JobConfig
from ..models.job_input import JobInput
from ..models.job_output import JobOutput, OutputTarget
from ..transformations import spark_impl
from ..transformations.transformations import validate_and_load_transformations
from ..transformations.spark_impl.da_validation_rules import run_validation
from ..writers.writer_factory import ScdWriter
from ..readers.factory import create_reader


class SparkEnv:
    """
    Contains additional context to be referred by transformations
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark


class SparkExecutor(Executor):
    def __init__(self, job_config: JobConfig, spark: SparkSession, glue_context=None):
        """
        :param glue_context: (GlueContext) is not required as it is passed to the module
            (reader, writers) that needs it only. It should be used only when absolutely needed
            (reading from Glue catalog e.g.) to avoid depending the whole framework on Glue.
            Most of the cases Spark should be enough.
        """
        super().__init__(job_config)
        self.spark = spark
        self.glue_context = glue_context
        self.spark_env = SparkEnv(spark)

    def _transform_and_validate(self, df: DataFrame, output: JobOutput) -> DataFrame:
        for trf in validate_and_load_transformations(output.transformations):
            df = self.execute_transformation(df, trf)

        for validation in output.validations:
            df = run_validation(validation, df)

        return df

    def _execute_single_input(self, output: JobOutput, no_write=False):
        job_input = [v for v in self.job_config.input if v.name == output.input_name]
        assert len(job_input) == 1, f"exact one input with name {output.input_name} must be provided"

        df_list = []

        for df in self.load_input_to_df(job_input[0]):
            df = self._transform_and_validate(df, output)
            df_list.append(df)
            # For debug purpose
            if no_write:
                continue
            for target in output.targets:
                self._write_output(df, target, self.glue_context)

        return df_list

    def _execute_multiple_inputs(self, output: JobOutput):
        job_inputs = [v for v in self.job_config.input if v.name in output.input_names]
        assert len(job_inputs) == len(output.input_names)

        input_df = {}

        for job_input in job_inputs:
            input_df[job_input.name] = []
            for df in self.load_input_to_df(job_input):
                df = self._transform_and_validate(df, output)

                input_df[job_input.name].append(df)

        for target in output.targets:
            self._write_output(input_df, target, self.glue_context)

    def execute_transformations(self, output: JobOutput, no_write=False):
        if output.input_names:
            return self._execute_multiple_inputs(output)
        else:
            return self._execute_single_input(output, no_write)

    def load_input_to_df(self, job_input: JobInput):
        reader = create_reader(job_input, self.spark, self.glue_context)
        return reader.read()

    def execute_transformation(self, df: DataFrame, trf: dict) -> DataFrame:
        trf = deepcopy(trf)
        impl = getattr(spark_impl, trf["name"], None)
        if not callable(impl):
            raise BaseException(f'Transformation {trf["name"]} is not supported by SPARK execution engine.')
        trf["spark_env"] = self.spark_env
        trf["job_config"] = self.job_config
        trf["glue_context"] = self.glue_context
        return impl(df, trf)

    def _write_output(
        self,
        df_or_dfs: Union[Dict[str, List[DataFrame]], DataFrame],
        target: OutputTarget,
        glue_context=None,
    ):
        # TODO: handle sub_folder_rule
        writer = ScdWriter(self.spark, df_or_dfs, target, self.job_config, glue_context)
        writer.write()

    @staticmethod
    def parse_schema(schema_json: dict):
        if not schema_json:
            return None
        return StructType.fromJson({"fields": schema_json})