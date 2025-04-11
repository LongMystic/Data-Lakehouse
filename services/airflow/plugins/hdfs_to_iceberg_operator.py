from airflow.plugins_manager import AirflowPlugin
from utils.spark_connections import get_spark_thrift_conn
from airflow.models import BaseOperator

import logging

_logger = logging.getLogger(__name__)

class HDFSToIcebergOperator(BaseOperator):
    def __init__(self, task_id: str,
                 hive_server2_conn_id,
                 sql="",
                 iceberg_table_name=None,
                 num_keep_retention_snaps=5,
                 iceberg_db="default",
                 *args,
                 **kwargs
    ):
        super().__init__(task_id, *args, **kwargs)
        self.hive_server2_conn_id = hive_server2_conn_id
        self.sql = sql
        self.iceberg_table_name = iceberg_table_name
        self.num_keep_retention_snaps = num_keep_retention_snaps
        self.iceberg_db = iceberg_db

    def get_spark_conn(self):
        conn = get_spark_thrift_conn(self.hive_server2_conn_id)
        return conn

    def create_tmp_table(self, cursor):
        create_tmp_table_sql = f"""
            CREATE TABLE default.{self.iceberg_table_name}_tmp
            USING parquet
            LOCATION '/raw/{self.iceberg_table_name}_tmp/'
        """