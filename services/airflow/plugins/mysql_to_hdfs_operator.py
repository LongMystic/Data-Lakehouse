from typing import Any

from airflow.operators.python import BaseOperator
from airflow.utils.context import Context
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.mysql.hooks.mysql import MySqlHook
import logging
import pyarrow
import pyarrow.parquet as pq
import subprocess

class MySQLToHDFSOperator(BaseOperator):
    def __init__(
            self, task_id: str,
            mysql_conn_id,
            spark_conn_id,
            hdfs_conn_id,
            hdfs_path,
            schema,
            sql: str = None,
            params: dict = None,
    ):
        super().__init__(task_id)
        self.mysql_conn_id = mysql_conn_id
        self.hdfs_conn_id = hdfs_conn_id
        self.spark_conn_id = spark_conn_id
        self.hdfs_path = hdfs_path
        self.schema = schema
        self.sql = sql
        self.params = params

    def fetch_data(self, mysql_conn_id, schema, sql):
        data = []
        try:
            logging.info(f"Using mysql connection id: {mysql_conn_id} with schema {schema}")
            mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id, schema=schema)
            mysql_conn = mysql_hook.get_conn()
            logging.info(f"Connect to mysql successfully!")

            cursor = mysql_conn.cursor()
            logging.info(f"Executing query: {sql}")
            cursor.execute(sql)
            data = mysql_hook.get_records(sql)
            logging.info(f"Fetching data successfully!")
            cursor.close()
            mysql_conn.close()
        except Exception as e:
            logging.error(e)

        return data

    def execute(self, context: Context) -> Any:
        data = []
        try:
            data = self.fetch_data(
                mysql_conn_id=self.mysql_conn_id,
                schema=self.schema,
                sql=self.sql
            )
        except Exception as e:
            logging.error(e)

        column = []
        table = pyarrow.table(data, column)

        pq.write_table(table, "./data.parquet")
        subprocess.run(f"hdfs dfs -put ./data.parquet hdfs://namenode:8020:/{self.hdfs_path}")
        subprocess.run("rm -f ./data.parquet")


class MySQLToHDFSOperatorPlugin(AirflowPlugin):
    name = "MySQLToHDFSOperatorPlugin"
