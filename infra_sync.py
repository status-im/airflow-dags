import sys
import json
from os import path
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import SimpleHttpOperator

# HACK: Fix for loading relative modules.
sys.path.append(path.dirname(path.realpath(__file__)))
from tasks.airbyte import fetch_airbyte_connections_tg
from providers.airbyte.operator import AirbyteTriggerSyncOperator

"""
DAG to sync data from Victor Ops 
"""

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

ARGS = { 
    'owner': 'apentori',
    'depends_on_past': False,
    'start_date': datetime(2024,4,30),
    'email': ['alexis@status.im'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
}

airbyte_connections=['victor_ops']

@dag('infra_sync', 
            default_args=ARGS, 
            schedule_interval='0 3 * * *')
def infra_sync():
    connections_id=fetch_airbyte_connections_tg(airbyte_connections)

    # Trigger Airbyte fetch Data from Github
    gh_sync_victor_ops = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_victor_ops',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['victor_ops'],
        asynchronous=False,
        wait_seconds=3
    )

    # Launch DBT transformation on the data previously fetched
    dbt_transform = BashOperator(
        task_id='dbt_run_models_infra',
        bash_command='dbt run --profiles-dir /dbt --project-dir /dbt/dbt-models/ --select infra'
    )
    connections_id >> gh_sync_victor_ops >> dbt_transform 

infra_sync()