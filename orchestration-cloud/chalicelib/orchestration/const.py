from enum import unique

from chalicelib.utils.utils import BaseEnum

AWS_REGION = "ap-southeast-1"
DEFAULT_DATE_FORMAT = "%Y%m%d"
LEGACY_G2I_DATE_FORMAT = "%Y-%m-%d"

COB_EVENT_DELAY = 90 * 60  # 90 minutes
MAX_SQS_DELAY_SECONDS = 900

MAX_CYCLE_COUNT_THRESHOLD = 12
DEFAULT_DELAY_SECONDS = 300

RESOURCE_MAX_RETRIES = 5
ONE_DAY_IN_SECONDS = 86400


@unique
class EventType(str, BaseEnum):
    MANUAL = "MANUAL"
    S3_NOTIFY_RAW = "S3_NOTIFY_RAW"
    S3_NOTIFY_GOLDEN = "S3_NOTIFY_GOLDEN"
    ORCS_EVENT = "ORCS_EVENT"
    SCHEDULED = "SCHEDULED"
    COB = "COB"
    DELAY = "DELAY"


@unique
class EventMeta(str, BaseEnum):
    PROCESS_TIMESTAMP = "process_timestamp"
    RESOURCE_RETRIES = "resource_retries"



@unique
class JobLogStatus(str, BaseEnum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
    SKIPPED = "SKIPPED"


@unique
class GlueArgsAdapter(str, BaseEnum):
    STANDARD = "standard"
    LEGACY_R2S_1TM_1B = "legacy_r2s_1tm_1b"
    LEGACY_R2S_CDC_1B = "legacy_r2s_cdc_1b"
    LEGACY_R2G_1B = "legacy_r2g_1b"
    LEGACY_G2I_1B = "legacy_g2i_1b"


@unique
class DataTimeType(str, BaseEnum):
    DATE = "date"


@unique
class LegacyJobReportStatus(str, BaseEnum):
    SUCCESS = "SUCCESS"
    FAIL = "FAIL"


@unique
class RetryPolicy(str, BaseEnum):
    BACKOFF = "backoff"


@unique
class RetryMode(str, BaseEnum):
    ALWAYS = "always"
    CANCEL_IF_LATEST_JOB_SUCCESS = "cancel_if_latest_job_success"


@unique
class Retried(int, BaseEnum):
    TRUE = 1
    FALSE = 0


@unique
class AuditAction(str, BaseEnum):
    CONDITION_FAILED = "CONDITION_FAILED"


@unique
class ResourceType(str, BaseEnum):
    DPU = "DPU"