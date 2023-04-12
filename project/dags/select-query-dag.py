import logging
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from dateutil.relativedelta import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@amdocs.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(0, 300)
}
dag = DAG(
    dag_id="",
    default_args=default_args,
    description='using snowflake hook',
    start_date=datetime(2023,3,9,0,0),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False
)
# CALL UPSERT_TO_PURGE_RUNTIME_AUDIT_STAGING();

def execute_snowflake_procedure():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    result = hook.get_first("select * from table(information_schema.task_history());")
    print(f"Result of the query: {result[0]}")

t0 = PythonOperator(
    task_id="snowflake-hook-dag",
    python_callable=execute_snowflake_procedure,
    op_kwargs={"my_param": 'executing-procedure-on-snowflake'},
    provide_context=True,
    do_xcom_push=True,
    dag = dag
)

t0
