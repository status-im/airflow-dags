import sys
import logging as LOG
from os import path
from datetime import datetime, timedelta
import logging
import sys

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.bash_operator import BashOperator


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

with DAG('test', default_args=ARGS, schedule_interval=None, catchup=False) as dag:
    task = BashOperator(
            task_id="test",
            bash_command='echo "test"')

    task

