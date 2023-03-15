from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 3, 1),
}

with DAG('snowflake_stored_proc_dag', default_args=default_args, schedule_interval=None) as dag:

    run_stored_proc = SnowflakeOperator(
        task_id='run_stored_proc',
        snowflake_conn_id='snowflake_conn',
        sql='CALL UPSERT_TO_PURGE_RUNTIME_AUDIT_STAGING();',
        autocommit=True
    )

    run_stored_proc
