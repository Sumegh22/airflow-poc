import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
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

def __init__(self, name=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = 'audit-tables-purging-dag'


default_args = {
    'owner': 'snowflake-audit-deployer',
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
def execute_snowflake_procedure():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    result = hook.get_first("CALL UPSERT_TO_PURGE_RUNTIME_AUDIT_STAGING();")
    print(f"Result of the query: {result[0]}")

t0 = PythonOperator(
    task_id="snowflake-hook-dag",
    python_callable=execute_snowflake_procedure,
    op_kwargs={"my_param": 'executing-procedure-on-snowflake'},
    provide_context=True,
    do_xcom_push=True
)

t0
