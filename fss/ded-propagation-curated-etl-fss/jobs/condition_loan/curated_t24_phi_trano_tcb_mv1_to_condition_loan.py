import logging
import sys
import datetime
import pytz
from datetime import date
from pyspark import StorageLevel
import pyspark.sql.functions as F
from joblib.job_execute import run, G2CIceBergJobExecuter
from joblib.writers.iceberg import scd2
from joblib.util.utils import read_dfs_by_inputs
from pyspark.sql.types import StringType,IntegerType
from joblib.constant import common_values as VAL
import uuid
from pyspark.sql import DataFrame, SparkSession

logger = logging.Logger(__file__)
logging.basicConfig(level=logging.INFO)



@F.udf
def create_uuid(name):
    return str(uuid.uuid5(uuid.NAMESPACE_OID, name))

class JobExecuter(G2CIceBergJobExecuter):
    def __init__(self, job_name, input, output, params, glue_context):
        super().__init__(job_name, input, output, params, glue_context)

    @property
    def transform(self):
        #data date
        self.data_date = self.params[VAL.DATA_DATE]
        # Dataframe Source
        df_t24_phi_tranoth_tcb_mv1 = self.rdfs["t24_phi_tranoth_tcb_mv1"]

        # Filtering data
        if self.data_date == VAL.COMMON_BLANK:  # Init Load
            df_t24_phi_tranoth_tcb_mv1 = df_t24_phi_tranoth_tcb_mv1.withColumn("ppn_dt", F.to_date(F.col("ds_partition_date"),"yyyy-MM-dd"))
        else:                     # Incremental Load
            self.data_date = self.params[VAL.DATA_DATE]
            df_t24_phi_tranoth_tcb_mv1 = (
                df_t24_phi_tranoth_tcb_mv1.filter(
                    (F.to_date(F.col("ds_partition_date"),"yyyy-MM-dd") == self.data_date)
                ).withColumn(
                    "ppn_dt",
                    F.to_date(F.lit(self.data_date),"yyyy-MM-dd")
                )
            )

        df_t24_phi_tranoth_tcb_mv1 = df_t24_phi_tranoth_tcb_mv1.withColumn(
                "eff_dt",
                F.to_date(F.lit(self.data_date),"yyyy-MM-dd")
        ).withColumn(
                "end_dt",
                F.to_date(F.lit("3000-11-08"), "yyyy-MM-dd")
        ).withColumn(
                "rec_st",
                F.lit("1").cast(IntegerType())
        ).withColumn(
                "ar_id",
                create_uuid(F.concat(F.lit('AR'),df_t24_phi_tranoth_tcb_mv1["ld_id"],F.lit('T24_LD')))
        ).withColumn(
                "cd_id",
                create_uuid(F.concat(F.lit('CD'),df_t24_phi_tranoth_tcb_mv1["ld_id"],F.lit('T24')))
        )

        df_t24_phi_tranoth_tcb_mv1.createOrReplaceTempView("t24_phi_tranoth_tcb_mv1_temp")
        
        sql = """
        select 
            p.ar_id as ar_id
            ,p.cd_id as cd_id 
            ,p.ld_id as ar_ctr_nbr
            ,to_date(p.from_date, 'yyyyMMdd') as from_dt
            ,to_date(p.to_date, 'yyyyMMdd') as to_dt
            ,cast(p.repayment_rate as decimal(38,6)) as repymt_rate
            ,p.repayment_type as repymt_tp
            ,concat(p.pos,'.', p.sub_pos) as rec_position
            ,p.eff_dt as eff_dt 
            ,p.end_dt as end_dt
            ,p.rec_st as rec_st
            ,'T24_LD' as src_stm_id
            ,p.ppn_dt as ppn_dt
        from t24_phi_tranoth_tcb_mv1_temp p
        """

        df_output = self.glue_context.sql(sql)
        return df_output

    def execute(self):
        self.read()
        output_df_scd2 = self.transform
        if self.params["data_date"] == VAL.COMMON_BLANK:
            dt = datetime.datetime.now(pytz.timezone("Asia/Ho_Chi_Minh"))
            process_date = (dt - datetime.timedelta(days=1)).strftime(VAL.DATE_FORMAT)
        else:
            process_date =datetime.datetime.strptime(self.params["data_date"], VAL.DATE_FORMAT).strftime(VAL.DATE_FORMAT)
        output_target_scd2 = self.job_config.output[0].targets[0]
        writer_scd2 = scd2.IcebergSCD2Writer(
           self.glue_context,self.glue_context.spark_session, output_df_scd2, output_target_scd2, process_date
        )
        writer_scd2.write()
    def read(self):
        self.rdfs = read_dfs_by_inputs(
            self.glue_context.spark_session, self.glue_context, self.job_config
        )


if __name__ == "__main__":
    run(sys.argv, JobExecuter, logger)