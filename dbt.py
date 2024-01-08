import sys
import logging as LOG
from os import path
from datetime import datetime, timedelta
import sys

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.bash_operator import BashOperator

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

ARGS = { 
    'owner': 'apentori',
    'depends_on_past': False,
    'start_date': datetime(2023,6,1),
    'email': ['alexis@status.im'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
}

@dag('DBT models', default_args=ARGS, schedule_interval=None)
def dbt_models():
    
    BashOperator(
        task_id = 'dbt_postgres_debug',
        bash_command='dbt debug --profiles-dir /dbt --project-dir /dbt/dbt-models'
    )
    BashOperator(
        task_id = 'dbt_postgres_test',
        bash_command='dbt test --profiles-dir /dbt --project-dir /dbt/dbt-models'
    )
    BashOperator(
       task_id='dbt_postgres_run',
       bash_command='dbt run --profiles-dir /dbt --project-dir /dbt/dbt-models'
    )

dbt_models()
