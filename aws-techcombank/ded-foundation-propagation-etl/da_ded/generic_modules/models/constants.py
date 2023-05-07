from .base_models import BaseEnum


class ExecutionEngines(BaseEnum):
    SPARK = "SPARK"


class SerializationFormats(BaseEnum):
    CSV = "CSV"
    PARQUET = "PARQUET"
    JSON = "JSON"
    iceberg = "iceberg"


class PartitionConfigs(BaseEnum):
    DAILY = "DAILY"
    MONTHLY = "MONTHLY"


class WriterType(BaseEnum):
    FACT = "Fact"
    FACT_V2 = "FactV2"
    FACT_V3 = "FactV3"
    FACT_V4 = "FactV4"
    DYNAMO_GLUE_FRAME_WRITER = "DYNAMO_GLUE_FRAME_WRITER"
    KINESIS_STREAM_WRITER = "KinesisStreamWriter"
    TYPE1 = "ScdType1"
    TYPE2 = "ScdType2"
    TYPE3 = "ScdType3"
    TYPE4 = "ScdType4"
    TYPE5 = "ScdType5"
    TYPE6 = "ScdType6"
    SIMPLE_WRITER = "SIMPLE_WRITER"
    SIMPLE_PARITION_WRITER = "SIMPLE_PARTITION_WRITER"
    SCD2_MIGRATE_TYPE7 = "Scd2MigrateType7"
    SCD2_MIGRATE_TYPE7_NO_DELETE = "Scd2MigrateType7NoDelete"
    SCD2_F_CDC5 = "SCD2FCDC5Writer"
    CMSCARD_TYPE2 = "CmsCardMigrateType7"
    CMSCUSTOMER_TYPE2 = "CmsCustomerMigrateType7"
    CMSCONTRACTSCYCLE_TYPE2 = "CmsContractscycleMigrateType7"
    REF_DATA = "RefDataWriter"
    T24_TYPE2 = "T24ScdType2"
    T24_TYPE2_CDC_TDS = "T24Scd2CDCTds"
    T24_TYPE2_DWH_TDS = "T24Scd2DwhTds"
    T24_TYPE2_DWH_SNP = "T24Scd2DwhSnp"
    T24_TYPE2_DWH_SNP_BACKFILL = "T24Scd2DwhSnpBackfill"
    T24_TYPE2_MIGRATION3B = "T24ScdType2Migration3b"
    T24_TYPE2_DWH_TDS_3B = "T24Scd2DwhTds3b"
    T24_TYPE2_MIGRATION4C_SNP = "T24Scd2MigrationType4cSnp"
    T24_TYPE2_MIGRATION4C_SNP_BACKDATE = "T24Scd2MigrationType4cSnpBackdate"

    # T24 SCD2 COB
    T24_TYPE2_COB = "T24ScdType2COB"
    T24_TYPE2_CDC_TDS_COB = "T24Scd2CDCTdsCOB"
    T24_TYPE2_DWH_TDS_COB = "T24Scd2DwhTdsCOB"
    T24_TYPE2_DWH_SNP_COB = "T24Scd2DwhSnpCOB"
    T24_TYPE2_MIGRATION3B_COB = "T24ScdType2Migration3bCOB"
    T24_TYPE2_DWH_TDS_3B_COB = "T24Scd2DwhTds3bCOB"
    T24_TYPE2_MIGRATION4C_SNP_COB = "T24Scd2MigrationType4cSnpCOB"
    T24_TYPE2_MIGRATION4C_SNP_BACKDATE_COB = "T24Scd2MigrationType4cSnpBackdateCOB"
    T24_TYPE2_COB_HIST_T24 = "T24Scd2CobHistT24"
    T24_TYPE2_COB_HIST_T24_CDC = "T24Scd2CobHistT24CDC"
    T24_TYPE2_COB_HIST_DWH_TDS = "T24Scd2CobHistDwhTds"
    T24_TYPE2_COB_HIST_DWH_SNP = "T24Scd2CobHistDwhSnp"
    T24_CUSTOMER_1TM = "T24Customer1TM"
    SIMPLE_WRITER_WITH_COB_DATE = "SIMPLE_WRITER_WITH_COB_DATE"

    T24_TYPE4_HIST = "T24ScdType4Hist"
    T24_TYPE4_CURRENT_FROM_STAGING = "T24ScdType4CurrentFromStaging"
    T24_TYPE4_FROM_SPEED_LAYER = "T24ScdType4FromSpeedLayer"
    T24_TYPE4_HIST_COB = "T24ScdType4HistCob"
    T24_TYPE4_CURRENT_COB = "T24ScdType4CurrentCob"
    T24_TYPE4_CURRENT_CUSTOMER_COB = "T24ScdType4CurrentCustomerCob"
    T24_TYPE4_CURRENT_COB_ALLOW_STAGING_FIELD_UPDATE = "T24ScdType4CurrentCobAllowStagingFieldUpdate" # noqa

    # T24 SCD4 COB BACKFILL
    T24_TYPE4_CURRENT_COB_BACKFILL = "T24ScdType4CurrentCobBackfill"
    T24_TYPE4_HIST_COB_BACKFILL = "T24ScdType4HistCobBackfill"
    T24_TYPE4_BACKFILL_FROM_HIST = "T24Scd4BackfillFromHist"

    # T24 SCD2 COB BACKFILL
    T24_TYPE2_COB_BACKFILL = "T24ScdType2COBBackfill"
    T24_TYPE2_CDC_TDS_COB_BACKFILL = "T24Scd2CDCTdsCOBBackfill"

    CUSTOM = "CUSTOM"
    # FACT UPSERT 1 <> T24
    FACT_UPSERT_1 = "FactUpsert1"

    #Simple writer with multiple partitions
    SIMPLE_WRITER_WITH_MULTIPLE_PARTITION = "SimpleWriterWithMultiplePartition"
    T24_TYPE4_CURRENT_FROM_STAGING_DYM_PARTITON = "T24ScdType4CurrentFromStagingDymPartition"
    
    #IceBergWriter
    ICEBERG_UPSERT = "IceBergUpsert"

    #DynamoDB Data Lineage to SCD2
    DYNAMO_LINEAGE_TO_SCD2 = "DynamoLineageToSCD2"

class WriteMode(BaseEnum):
    APPEND_ONLY = "append"
    OVERWRITE = "overwrite"
    OVERWRITE_PARTITION = "overwrite_partition"


class Table(BaseEnum):
    BASE_TABLE = "base_table"
    HIST_TABLE = "hist_table"


class SubFolderRules(BaseEnum):
    USE_INPUT_ENTRY_FILE_DATE = "USE_INPUT_ENTRY_FILE_DATE"


class RefDataWriterType(BaseEnum):
    IDENTITY_YEARBIRTH = "RefValidateIdentityIssuedDateWriter"


class ReaderType(BaseEnum):
    T24_STAGING = "t24_staging"
    JDBC = "jdbc"
    RAW = "raw"
    CATALOG = "catalog"
    XML_TAX = 'xml_tax'
    CATALOG_SQL = 'catalog_sql'
    SPARK_SQL = 'spark_sql'
    SFORCE_API = 'sforce_api'
    DYNAMO_LINEAGE = "dynamo_datalineage"

class RecordChangeType(BaseEnum):
    T24_1TM = "T24_1TM"
    DWH_1TM = "DWH_1TM"
    CDC_UPDATE = "CDC_UPDATE"
    CDC_INSERT = "CDC_INSERT"
    CDC_DELETE = "CDC_DELETE"
    CDC_TRUNCATE = "CDC_TRUNCATE"
    T24_BACKFILL = "T24_BACKFILL"

    @classmethod
    def cdc_types(cls):
        return {cls.CDC_UPDATE, cls.CDC_INSERT, cls.CDC_DELETE, cls.CDC_TRUNCATE}

    @classmethod
    def cdc_upsert_types(cls):
        return {cls.CDC_UPDATE, cls.CDC_INSERT}

    @classmethod
    def onetm_types(cls):
        return {cls.T24_1TM, cls.DWH_1TM}


class DsColumn(BaseEnum):
    RECORD_CREATED_TIMESTAMP = "ds_record_created_timestamp"
    RECORD_UPDATED_TIMESTAMP = "ds_record_updated_timestamp"
    RECORD_STATUS = "ds_record_status"
    RECORD_IS_DELETED = "ds_record_is_deleted"
    RECORD_CHANGE_TYPE = "ds_record_change_type"
    PARTITION_DATE = "ds_partition_date"
    LOAD_DATE = "ds_load_date"
    CDC_INDEX = "ds_cdc_index"
    PARTITION_RECID = "ds_partition_recid"
    PROCESS_DATE = "ds_process_date"
    RECORD_SOURCE = "ds_record_source"
    COB_DATE = "ds_cob_date"

    @classmethod
    def golden_t24_scd2_columns(cls):
        return (
            cls.RECORD_CREATED_TIMESTAMP,
            cls.RECORD_UPDATED_TIMESTAMP,
            cls.RECORD_STATUS,
            cls.RECORD_CHANGE_TYPE,
            cls.PARTITION_DATE,
            cls.CDC_INDEX,
        )

    @classmethod
    def golden_t24_scd2_cob_columns(cls):
        return (*cls.golden_t24_scd2_columns(), cls.COB_DATE)

    @classmethod
    def golden_t24_scd2_migration3_columns(cls):
        return (*cls.golden_t24_scd2_columns(), cls.RECORD_SOURCE)

    @classmethod
    def golden_t24_scd2_migration3_cob_columns(cls):
        return (*cls.golden_t24_scd2_migration3_columns(), cls.COB_DATE)

    @classmethod
    def golden_t24_scd4_hist_columns(cls):  # NOSONAR
        return (
            cls.RECORD_CREATED_TIMESTAMP,
            cls.PARTITION_DATE,
            cls.RECORD_CHANGE_TYPE,
            cls.CDC_INDEX,
        )

    @classmethod
    def golden_t24_scd4_current_columns(cls):
        return (
            cls.RECORD_CREATED_TIMESTAMP,
            cls.PARTITION_DATE,
            cls.RECORD_IS_DELETED,
            cls.CDC_INDEX,
            cls.PARTITION_RECID,
        )

    @classmethod
    def golden_t24_scd2_snapshot_columns(cls):
        return (cls.RECORD_CREATED_TIMESTAMP, cls.PROCESS_DATE)

    @classmethod
    def golden_t24_scd2_4c_snapshot_columns(cls):
        return (
            cls.PROCESS_DATE,
            cls.PARTITION_DATE,
            cls.RECORD_CREATED_TIMESTAMP,
            cls.CDC_INDEX,
            cls.RECORD_CHANGE_TYPE,
        )

    @classmethod
    def golden_t24_scd2_cob_hist_columns(cls):
        return (
            cls.RECORD_CREATED_TIMESTAMP,
            cls.RECORD_CHANGE_TYPE,
            cls.PARTITION_DATE,
            cls.CDC_INDEX,
        )

    @classmethod
    def golden_t24_scd4_cob_hist_columns(cls):  # NOSONAR
        return (
            cls.RECORD_CREATED_TIMESTAMP,
            cls.PARTITION_DATE,
            cls.RECORD_CHANGE_TYPE,
            cls.CDC_INDEX,
        )

    @classmethod
    def golden_t24_scd4_cob_current_columns(cls):
        return (
            cls.RECORD_CREATED_TIMESTAMP,
            cls.PARTITION_DATE,
            cls.RECORD_IS_DELETED,
            cls.CDC_INDEX,
            cls.PARTITION_RECID,
            cls.RECORD_CHANGE_TYPE,
        )

    @classmethod
    def golden_fact_columns(cls):
        return (cls.RECORD_CHANGE_TYPE, cls.PARTITION_DATE)

    @classmethod
    def golden_fact_upser_1_columns(cls):
        return (cls.RECORD_UPDATED_TIMESTAMP, cls.PARTITION_DATE)


class JdbcSupportedConnectionTypes(BaseEnum):
    POSTGRESQL = "postgresql"


class RecordStatus(BaseEnum):
    ACTIVE = 1
    INACTIVE = 0


class MigrationType(BaseEnum):
    MIGRATE_1 = "migration_type_1"
    MIGRATE_2 = "migration_type_2"
    MIGRATE_3A = "migration_type_3a"
    MIGRATE_3B = "migration_type_3b"
    MIGRATE_4A = "migration_type_4a"
    MIGRATE_4B = "migration_type_4b"
    MIGRATE_4C = "migration_type_4c"
    MIGRATE_5 = "migration_type_5"
    MIGRATE_6 = "migration_type_6"
    MIGRATE_7 = "migration_type_7"
    MIGRATE_8 = "migration_type_8"
    MIGRATE_9 = "migration_type_9"


class Regex(BaseEnum):
    DATE_PREFIX = r"(date=.*)/"


class DateFormat(BaseEnum):
    DEFAULT = "%Y%m%d"