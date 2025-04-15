import logging

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator

from mysql_to_hdfs_operator import MySQLToHDFSOperator
from mysql_to_hdfs_operator_v2 import MySQLToHDFSOperatorV2
from hdfs_to_iceberg_operator import HDFSToIcebergOperator
from iceberg_operator import IcebergOperator

from schema.sales.schema_raw import ALL_TABLES

from utils.utils import get_variables, generate_table_properties_sql
DAG_NAME = "mysql_to_iceberg_daily"
variable = get_variables(DAG_NAME)

def load_raw(task_group_id, **kwargs):
    with TaskGroup(task_group_id) as task_group:
        spark_conn_id = kwargs.get("spark_conn_id")
        mysql_conn_id = kwargs.get("mysql_conn_id")
        # task_load_raw = MySQLToHDFSOperator(
        #     task_id = f"load_table_to_raw",
        #     schema="test",
        #     sql="SELECT * FROM test.category",
        #     spark_conn_id=spark_conn_id,
        #     mysql_conn_id=mysql_conn_id,
        #     hdfs_path="/raw/test/category"
        # )
        
        task_load_raw = MySQLToHDFSOperatorV2(
            task_id = f"load_table_to_raw",
            schema="test",
            sql="SELECT * FROM test.category",
            mysql_conn_id=mysql_conn_id,
            hdfs_path="/raw/category_tmp"
        )
        task_load_raw

        # task = PythonOperator(
        #     task_id="python",
        #     python_callable=python_callable
        # )
        # task
    return task_group


def python_callable():
    logging.info(variable)

def load_staging(task_group_id, **kwargs):
    with TaskGroup(task_group_id) as task_group:
        spark_conn_id = kwargs.get("spark_conn_id")

        for tbl in ALL_TABLES:
            task_load_staging = HDFSToIcebergOperator(
                task_id=f"load_table_to_staging",
                iceberg_table_name="category",
                num_keep_retention_snaps=5,
                iceberg_db="longvk_test",
                spark_conn_id=spark_conn_id,
                table_properties=generate_table_properties_sql(tbl)
            )
        task_load_staging

        # task = PythonOperator(
        #         task_id="python",
        #         python_callable=python_callable
        #     )
        # task
    return task_group


def load_warehouse(task_group_id, **kwargs):
    with TaskGroup(task_group_id) as task_group:
        spark_conn_id = kwargs.get("spark_conn_id")
        task_load_warehouse = IcebergOperator(
            task_id=f"load_table_to_warehouse",
            iceberg_table_name="category",
            num_keep_retention_snaps=5,
            iceberg_db="longvk_test",
            spark_conn_id=spark_conn_id
        )
        task_load_warehouse
        return task_group


def clean_raw(task_group_id, **kwargs):
    with TaskGroup(task_group_id) as task_group:
        # table_name = 
        task_clean_raw = BashOperator(
            task_id=f"clean_raw",
            bash_command="hdfs dfs -rm -r /raw/test/category/"
        )
        task_clean_raw
        return task_group