from abc import abstractmethod

from ..models.job_config import JobConfig
from ..models.job_output import JobOutput


class Executor:
    def __init__(self, job_config: JobConfig) -> None:
        self.job_config = job_config

    def execute(self):
        for output in self.job_config.output:
            self.execute_transformations(output)

    @abstractmethod
    def execute_transformations(self, output: JobOutput):
        pass