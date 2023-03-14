import logging
from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner": "Airflow", "start_date": datetime(2021,3,1)}

dag = DAG(
    dag_id="audit-table-query", default_args=args, schedule_interval=None
)

query = ["""call UPSERT_TO_PURGE_RUNTIME_AUDIT();"""
]


def count1(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    result = dwh_hook.get_first("call UPSERT_TO_PURGE_RUNTIME_AUDIT();")
    logging.info("Data in the audit staging table", result)


with dag:
    query_exec = SnowflakeOperator(
        task_id="snowfalke_task1",
        sql=query,
        snowflake_conn_id="snowflake_conn",
    )

    count_query = PythonOperator(task_id="count_query", python_callable=count1)
query_exec >> count_query