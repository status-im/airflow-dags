import sys
import json
from os import path
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.decorators import dag, task

from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

# HACK: Fix for loading relative modules.
sys.path.append(path.dirname(path.realpath(__file__)))
from tasks.airbyte import fetch_airbyte_connections_tg
from providers.airbyte.operator import AirbyteTriggerSyncOperator

"""
DAG to fetch data from the different repo organisation in GitHub
"""

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

airbyte_connections=['gh_sync_logos_repos']

@dag('github_repo_extraction',
         default_args=ARGS,
         schedule_interval='30 */6 * * *')
def github_repo_extraction():
    connections_id=fetch_airbyte_connections_tg(airbyte_connections)


    AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_github',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['gh_sync_logos_repos'],
        asynchronous=False,
        wait_seconds=3
    )

github_repo_extraction()
