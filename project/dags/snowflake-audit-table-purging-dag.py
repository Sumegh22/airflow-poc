from datetime import datetime
from datetime import timedelta
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@amdocs.com'],
    'retries': 0,
    'email_on_failure': False
}


dag = DAG(
    dag_id="snowflake-audit-table-purging-dag",
    default_args=default_args,
    description='using snowflake hook',
    start_date=datetime(2023,3,9,0,0),
    schedule_interval='@daily',
    catchup=False,
    is_paused_upon_creation=False
)

def execute_snowflake_procedure():
    dagLoggingLevel = 'debug'
    try:
        t=datetime.datetime(delete_records_before)
        epoch = calendar.timegm(t.timetuple())
        print(epoch)
        if (dagLoggingLevel=='debug'):
            for logger_name in ['snowflake.connector']:
                logger = logging.getLogger(logger_name)
                logger.setLevel(logging.DEBUG)
                ch = logging.FileHandler('/tmp/python_connector.log')
                ch.setLevel(logging.DEBUG)
                ch.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s'))
                logger.addHandler(ch)
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')

        try:
            result = hook.get_first("call UPSERT_TO_PURGE_RUNTIME_AUDIT();")
            print(f"fetching result from snowflake : ")
            for row in result:
                    print(row)
                    if 'error' in row:
                        raise AirflowException(f'received "error" while executing procedure on your db at snowflake warehouse! Failing the DAG.')

        except Exception as e:
            print(f"Unknown Exception: {str(e)}")
            raise e

    except AirflowException as e:
        print(f"Airflow Exception: {str(e)}")
        raise e


t0 = PythonOperator(
    task_id="execute_procedure_on_snowflake",
    python_callable=execute_snowflake_procedure,
    op_kwargs={"my_param": 'executing-procedure-on-snowflake'},
    provide_context=True,
    dag=dag
)

t1 = PythonOperator(
    task_id="get-query-result",
    python_callable=lambda ti: ti.xcom_pull(task_ids="execute_procedure_on_snowflake"),
    provide_context=True,
    dag=dag
)

t0 >> t1