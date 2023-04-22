import abc
import json
import sys
from copy import deepcopy
from datetime import datetime
from typing import Type, List

from pyspark import SparkContext
from pyspark.sql import DataFrame

from joblib.writers.iceberg import scd2, scd4current
from .models.job_config import CuratedJobConfig
from .util.utils import read_dfs_by_inputs


class G2CIceBergJobExecuter(object):
    def __init__(self, job_name, input, output, params, glue_context):
        from awsglue.context import GlueContext

        self.input = input
        self.output = output
        self.params = params

        self.glue_context: GlueContext = glue_context
        if glue_context is not None:
            self.glue_context.spark_session.conf.set(
                "spark.sql.session.timeZone", "Asia/Ho_Chi_Minh"
            )
            self.glue_context.spark_session.conf.set(
                "spark.sql.iceberg.handle-timestamp-without-timezone", "true"
            )
        self.job_config = CuratedJobConfig(
            {
                "id": job_name + "_" + str(datetime.now()).replace(" ", "_"),
                "params": params,
                "input": input,
                "output": output,
            }
        )

    def read(self):
        self.rdfs = read_dfs_by_inputs(
            self.glue_context.spark_session, self.glue_context, self.job_config
        )

    def create_table_from_dataframe(
        self, df: DataFrame, database_name: str, table_name: str
    ):
        df.createOrReplaceTempView(f"tmp_{table_name}")

        query = f"""
        CREATE TABLE glue_catalog.{database_name}.{table_name}
        USING iceberg
        PARTITIONED BY (`ppn_dt`)
        AS SELECT * FROM tmp_{table_name}
        """
        print("Execute query:", query)
        self.glue_context.spark_session.sql(query)

    def get_scd2_writer(self, df, target):
        data_date = self.params[VAL.DATA_DATE]
        writer_scd2 = scd2.IcebergSCD2Writer(
            self.glue_context,
            self.glue_context.spark_session,
            df,
            target,
            data_date
        )
        return writer_scd2.write

    def get_scd4_writer(self, df, target):
        data_date = self.params[VAL.DATA_DATE]
        writer_scd4current = scd4current.IcebergSCD4CurrentWriter(
            self.glue_context,
            self.glue_context.spark_session,
            df,
            target,
            data_date
        )
        return writer_scd4current.write

    def check_table_exist(self, database: str, table: str):
        try:
            return (
                self.glue_context.sql(
                    f"select * from glue_catalog.{database}.{table}"
                ).count()
                >= 0
            )
        except Exception as ex:
            print(ex)
            return False

    def write_scd_with_writer(self, df: DataFrame, target: dict, writer):
        # target :
        # {
        #     "catalog_name": "glue_catalog",
        #     "tbl_name": "condition_loan",
        #     "db_name": "{{ prefix_curated }}-golden_curated",
        #     "writer_type": "CUSTOM",
        #     "serde": {
        #         "format": "iceberg"
        #     },
        #     "primary_columns": [
        #                     "ar_ctr_nbr",
        #                     "rec_position",
        #                     "src_stm_id"
        #     ],
        #     "src_stms": ["src_stm_id"]
        # }
        config = deepcopy(target.output_target_dict)
        table_config = dict((k, v) for k, v in config.items() if v is not None)

        database_name = table_config["db_name"]
        table_name = table_config["tbl_name"]

        if self.check_table_exist(database_name, table_name):
            writer()
        else:
            self.create_table_from_dataframe(df, database_name, table_name)

    @abc.abstractmethod
    def execute(self):
        pass


def run(
    arguments: List[str],
    job_executor_callable: Type[G2CIceBergJobExecuter],
    logger=None,
    conf=None,
):
    """
    Common way to run golden2insight Glue ETL for ETL script to import to avoid duplication

    """
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job

    if not logger:
        import logging

        logging.basicConfig(level=logging.INFO)
        logger = logging.Logger(__file__)
        logger.setLevel(logging.INFO)
        logger.addHandler(logging.StreamHandler(sys.stdout))

    args = getResolvedOptions(arguments, ["JOB_NAME", "Inputs", "Outputs", "Params"])

    params_str = args.get("Params")
    params = json.loads(params_str)

    input_data_str = args.get("Inputs")
    input_data = json.loads(input_data_str)
    logger.info(f"input_data {input_data}")

    output_data_str = args.get("Outputs")
    output_data = json.loads(output_data_str)
    logger.info(f"output_data {output_data}")
    sc = SparkContext(conf=conf)
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    job_executor_callable(
        args["JOB_NAME"], input_data, output_data, params, glue_context
    ).execute()
    job.commit()