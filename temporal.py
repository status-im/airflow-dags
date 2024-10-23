import sys
import json
from os import path
from datetime import datetime, timedelta
import logging
import time
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import SimpleHttpOperator

# HACK: Fix for loading relative modules.
sys.path.append(path.dirname(path.realpath(__file__)))
from tasks.airbyte import fetch_airbyte_connections_tg
from providers.airbyte.operator import AirbyteTriggerSyncOperator

"""
DAG to create Temporal data
"""

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

ARGS = {
    'owner': 'apentori',
    'depends_on_past': False,
    'start_date': datetime(2024,10,24),
    'email': ['alexis@status.im'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
    'catchup': False,
}

@dag(
    'temporal_data',
    default_args=ARGS,
    schedule_interval='0 */24 * * * '
)
def temporal_data_generation():

    dbt_run_temporal = BashOperator(
        task_id='dbt_run_models_temporal',
        bash_command='dbt run --profiles-dir /dbt --project-dir /dbt/dbt-models/ --select temporal'
    )

    dbt_run_temporal

temporal_data_generation()
