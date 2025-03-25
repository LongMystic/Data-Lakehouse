from airflow.utils.task_group import TaskGroup
from plugins.mysql_to_hdfs_operator import MySQLToHDFSOperator
# from plugins.iceberg_operator import IcebergOperator


def load_raw(task_group_id, **kwargs):
    with TaskGroup(task_group_id) as task_group:
        mysql_conn_id = kwargs.get("mysql_conn_id")
        spark_conn_id = kwargs.get("spark_conn_id")
        hdfs_conn_id = kwargs.get("hdfs_conn_id")

        task_load_raw = MySQLToHDFSOperator(
            mysql_conn_id=mysql_conn_id,
            spark_conn_id=spark_conn_id,
            hdfs_conn_id=hdfs_conn_id,
        )

        task_load_raw
    return task_group


def load_staging(task_group_id, **kwargs):
    with TaskGroup(task_group_id) as task_group:
        pass


def load_warehouse(task_group_id, **kwargs):
    with TaskGroup(task_group_id) as task_group:
        pass


def clean_raw(task_group_id, **kwargs):
    with TaskGroup(task_group_id) as task_group:
        pass