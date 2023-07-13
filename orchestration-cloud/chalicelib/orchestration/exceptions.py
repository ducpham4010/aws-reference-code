import json


class BadArgumentException(Exception):
    pass


class UnexpectedEventException(Exception):
    pass


class ResourceLockException(Exception):
    def __init__(self, job_id, resource_pool, desired_capacity, retries):
        self.job_id = job_id
        self.resource_pool = resource_pool
        self.desired_capacity = desired_capacity
        self.retries = retries

    def __str__(self):
        return f"unable to lock resource to job: {self.job_id}," \
               f" resource pool: {json.dumps(self.resource_pool)}" \
               f" desired capacity: {self.desired_capacity}" \
               f" after {self.retries} times retry"


class ResourceExceededMaxRetries(Exception):
    pass