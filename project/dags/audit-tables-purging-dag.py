import logging
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
from airflow.utils.decorators import apply_defaults
from dateutil.relativedelta import *
from datetime import datetime
from typing import Dict, Optional
from typing import Any,Dict, Optional
from airflow.exceptions import AirflowException



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@apply_defaults
def __init__(
        self,
        *,
        application_name: str,
        attach_log: bool = False,
        namespace: Optional[str] = None,
        snowflake_conn_id: str ="snowflake_default",
        api_group: str = 'batch',
        api_version: str = 'v1',
        **kwargs,
) -> None:
    super().__init__(**kwargs)
    self.application_name = 'audit-tables-purging-dag'
    self.attach_log = attach_log
    self.namespace = namespace
    self.snowflake_conn_id = snowflake_conn_id
    self.hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    self.api_group = api_group
    self.api_version = api_version

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023,1,1),
    'depends_on_past': False,
    'catchup': False,
    'email': ['airflow@amdocs.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 1,
    'retry_delay': timedelta(0, 300),
    'datastore_secret_name': 'aia-datastore-db-secret',
    'application_name':'audit-tables-purging-dag'
}


dag = DAG(
    'audit-tables-purging-dag',
    default_args=default_args,
    description='Query Snowflake using Snowflake hook',
    schedule_interval=None
)

def execute_snowflake_procedure():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    result = hook.get_first("CALL UPSERT_TO_PURGE_RUNTIME_AUDIT_STAGING();")
    print(f"Result of the query: {result[0]}")

with dag:
    query_task = PythonOperator(
        task_id='execute_snowflake_procedure',
        python_callable=execute_snowflake_procedure
    )