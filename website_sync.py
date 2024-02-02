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
DAG to sync data from github to the Website API 
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

airbyte_connections=['gh_sync_website_repos', 'load_hasura_website']

@dag('github_website_sync', 
            default_args=ARGS, 
            schedule_interval='15 */3 * * *')
def github_website_sync():
    connections_id=fetch_airbyte_connections_tg(airbyte_connections)

    # Trigger Airbyte fetch Data from Github
    AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_github',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['gh_sync_website_repos'],
        asynchronous=False,
        wait_seconds=3
    )

    # Launch DBT transformation on the data previously fetched
    BashOperator(
        task_id='dbt_postgres_run',
        bash_command='dbt run --profiles-dir /dbt --project-dir /dbt/dbt-models/ --select github'
    )
    # Trigger Airbyte Sync from main database to Hasura
    AirbyteTriggerSyncOperator(
        task_id='airbyte_sync_hasura',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['load_hasura_website'],
        asynchronous=False,
        wait_seconds=3
    )

github_website_sync()
