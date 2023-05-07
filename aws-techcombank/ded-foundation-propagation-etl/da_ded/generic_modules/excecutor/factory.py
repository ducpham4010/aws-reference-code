from ..models.job_config import JobConfig
from ..models.constants import ExecutionEngines
from .spark_executor import SparkExecutor


def create_executor(json_config: dict, spark=None, glue_context=None):
    config = JobConfig(json_config)

    if config.execution_engine == ExecutionEngines.SPARK:
        assert spark
        return SparkExecutor(config, spark, glue_context)
    else:
        raise NotImplementedError(f"Execution engine {config.execution_engine} is not implemented.")