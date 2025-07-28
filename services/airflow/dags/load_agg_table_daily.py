import logging

import pendulum
from airflow.models import DAG

from task_group.task_group import load_agg_warehouse
from datetime import timedelta, datetime
from utils.utils import get_variables
from utils.constant import *
from utils.notify_telegram import notify_success, notify_failure

DAG_NAME = "load_agg_table_daily"

SCHEDULE_INTERVAL = "00 02 * * *"

default_args = {
    "owner": "airflow",
    "start_date": pendulum.today("UTC").add(days=-1),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

variable = get_variables(DAG_NAME)

with DAG(
    dag_id=DAG_NAME,
    schedule_interval=SCHEDULE_INTERVAL,
    default_args=default_args,
    max_active_tasks=MAX_ACTIVE_TASKS,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=["daily", "longvk"],
    on_success_callback=notify_success,
    on_failure_callback=notify_failure

) as dag:
    logging.info(str(variable))

    task_load_to_agg_warehouse = load_agg_warehouse(
        task_group_id="etl_layer_business_agg",
        **variable
    )

    (
        task_load_to_agg_warehouse
    )
