from da_ded.utils import get_secret
from da_ded.logger import logger

from .base import BaseReader


class JdbcReader(BaseReader):
    def read(self):
        jdbc_config = self.job_input.jdbc_config
        pg_secrets = get_secret(jdbc_config["aws_secret_identifier"])
        pg_uri = (
            f"jdbc:postgresql://{pg_secrets['read_only_endpoint']}:{pg_secrets['port']}/{pg_secrets['database_name']}"
        )
        pg_options = {
            "url": pg_uri,
            "user": pg_secrets["username"],
            "password": pg_secrets["password"],
            "dbtable": jdbc_config["table_name"],
        }
        logger.info("Reading DF from JDBC ...")
        dynamic_frame = self.glue_context.create_dynamic_frame.from_options(
            connection_type="postgresql", connection_options=pg_options
        )
        df = dynamic_frame.toDF()
        logger.info("Read DF from JDBC with schema:\n")
        logger.info(df.dtypes)
        yield df