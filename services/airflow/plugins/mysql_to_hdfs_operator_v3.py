from typing import Any

from airflow.operators.python import BaseOperator
from airflow.utils.context import Context
from airflow.plugins_manager import AirflowPlugin
from utils.spark_connections import get_spark_thrift_conn
from airflow.hooks.base import BaseHook

import logging

_logger = logging.getLogger(__name__)


class MySQLToHDFSOperatorV3(BaseOperator):
    def __init__(
            self,
            mysql_conn_id="mysql_conn_id",
            spark_conn_id="spark_conn_id",
            hdfs_path=None,
            schema=None,
            table=None,
            sql: str = None,
            jdbc_options: dict = None,
            params: dict = {},
            partition_column: str = None,
            num_partitions: int = 4,
            lower_bound: int = None,
            upper_bound: int = None,
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.spark_conn_id = spark_conn_id
        self.hdfs_path = hdfs_path
        self.schema = schema
        self.table = table
        self.sql = sql
        self.params = params
        self.partition_column = partition_column
        self.num_partitions = num_partitions
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound

    def execute(self, context: Context) -> Any:

        _logger.info(f"Using MySQL connection id: {self.mysql_conn_id} with schema {self.schema}")
        _logger.info(f"Using Spark connection id: {self.spark_conn_id}")
        _logger.info(f"Using HDFS path to write data: {self.hdfs_path}")

        conn = get_spark_thrift_conn(self.spark_conn_id)
        cursor = conn.cursor()

        mysql_conn = BaseHook.get_connection(self.mysql_conn_id)

        _logger.info(f"Using SQL PATH: {self.sql}")

        if self.sql is None or self.sql == "":
            _logger.info("Sql query is empty, use \"SELECT * FROM schema.table\" as default query")
            self.sql = f"{self.schema}.{self.table}"
        else:
            self.sql = f"/opt/airflow/dags{self.sql}"
            with open(self.sql, 'r') as f:
                self.sql = f.read()
                for param in self.params:
                    self.sql = self.sql.replace(f"{{{param}}}", self.params[param])
            self.sql = f"({self.sql}) as filtered_data"
            _logger.info(f"Using SQL file: {self.sql}")

        # Build JDBC options for partitioning
        jdbc_options = {
            "url": f"jdbc:mysql://{mysql_conn.host}:{mysql_conn.port}/{self.schema}",
            "dbtable": self.sql,
            "user": mysql_conn.login,
            "password": mysql_conn.password,
        }

        # Add partitioning options if partition_column is provided
        if self.partition_column:
            jdbc_options.update({
                "partitionColumn": self.partition_column,
                "numPartitions": str(self.num_partitions),
            })
            if self.lower_bound is not None and self.upper_bound is not None:
                jdbc_options.update({
                    "lowerBound": str(self.lower_bound),
                    "upperBound": str(self.upper_bound),
                })

        # Convert jdbc_options to string format for Spark SQL
        jdbc_options_str = ",\n".join([f"'{k}' '{v}'" for k, v in jdbc_options.items()])

        spark_query = f"""
            SET spark.sql.legacy.allowNonEmptyLocationInCTAS=true;
            
            CREATE OR REPLACE TEMPORARY VIEW {self.table}_view
            USING org.apache.spark.sql.jdbc
            OPTIONS (
              {jdbc_options_str}
            );
            
            DROP TABLE IF EXISTS {self.table}_tmp;
            CREATE TABLE {self.table}_tmp
            USING parquet
            OPTIONS (path '{self.hdfs_path}')
            AS SELECT * FROM {self.table}_view;
            
            DROP VIEW IF EXISTS {self.table}_view
        """

        for query in spark_query.split(';'):
            _logger.info("Executing query %s\n", query)
            cursor.execute(query)

        _logger.info("Data successfully written to HDFS as Parquet.")


class MySQLToHDFSOperatorV3Plugin(AirflowPlugin):
    name = "mysql_to_hdfs_operator_v3_plugin"
    operators = [MySQLToHDFSOperatorV3]