from datetime import datetime
from datetime import date
from datetime import timedelta
import calendar
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowException


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
    dag_id="original-dag-to-be-used",
    default_args=default_args,
    description='using snowflake hook',
    start_date=datetime(2023,3,9,0,0),
    schedule_interval='@daily',
    catchup=False,
    is_paused_upon_creation=False
)

def execute_snowflake_procedure(**kwargs):
    logical_date = kwargs["logical_date"]
    print(f"Logical date is ", logical_date)
    retain_data_till = kwargs['dag_run'].conf.get('retain_data_till')
    print(f"retain_data_till ", retain_data_till)
    LoggingLevel = kwargs['dag_run'].conf.get('LoggingLevel')
    print(f"LoggingLevel", LoggingLevel)
    retries = kwargs['dag_run'].conf.get('retries')
    print(f"setting retires to ", retries)

    try:
        if (LoggingLevel=='debug'):
            for logger_name in ['snowflake.connector']:
                logger = logging.getLogger(logger_name)
                logger.setLevel(logging.DEBUG)
                ch = logging.FileHandler('/tmp/python_connector.log')
                ch.setLevel(logging.DEBUG)
                ch.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s'))
                logger.addHandler(ch)
        t = date.today()
        retain_till = (t - timedelta(days=retain_data_till, hours=0))
        print(f"This purge execution will delete all the data before :::", retain_till)
        epoch = calendar.timegm(retain_till.timetuple())
        print(f"Conversion of retention period in AUDIT_TIME for snowflake table", epoch)
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        try:
            result = hook.get_first("call UPSERT_TO_PURGE_RUNTIME_AUDIT(epoch);")
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

t1 = PythonOperator(
    task_id="execute_procedure_on_snowflake",
    python_callable = execute_snowflake_procedure,
    op_kwargs={"my_param": 'executing-procedure-on-snowflake'},
    provide_context = True,
    dag=dag
)

t2 = PythonOperator(
    task_id="get-query-result",
    python_callable=lambda ti: ti.xcom_pull(task_ids="execute_procedure_on_snowflake"),
    provide_context=True,
    dag=dag
)

t1 >> t2