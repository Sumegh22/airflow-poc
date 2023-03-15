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
    dag_id="audit-table-puring-dag", default_args=args, schedule_interval=None
)

query = ["""select count(*) from RDA_T1_DEV.public.RUNTIME_AUDIT""",
        ]


def count(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    result = dwh_hook.get_first("select count(*) from RDA_T1_DEV.public.RUNTIME_AUDIT")
    logging.info("Number of rows in `RDA_T1_DEV.public.RUNTIME_AUDIT`  - %s", result[0])


with dag:
    query_exec = SnowflakeOperator(
        task_id="snowfalke_task1",
        sql=query,
        snowflake_conn_id="snowflake_conn",
    )

    count_query = PythonOperator(task_id="count_query", python_callable=count)
query_exec >> count_query