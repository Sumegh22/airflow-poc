import logging
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023,1,1),
    'depends_on_past': False,
    'email': ['airflow@amdocs.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 1,
    'retry_delay': timedelta(0, 300),
    'datastore_secret_name': 'aia-datastore-db-secret'
}


with DAG('snow-flake-query-dag', default_args=default_args, schedule_interval=None) as dag:

    run_stored_proc = SnowflakeOperator(
        task_id='run_stored_proc',
        snowflake_conn_id='snowflake_conn',
        sql='CALL UPSERT_TO_PURGE_RUNTIME_AUDIT_STAGING();',
        autocommit=True
    )

    run_stored_proc