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
DAG to sync data from github for the Logos Org Map
"""

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

ARGS = { 
    'owner': 'apentori',
    'depends_on_past': False,
    'start_date': datetime(2024,2,20),
    'email': ['alexis@status.im'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
    'catchup': False,
}

airbyte_connections=[
    'gh_sync_vac_repos',
    'gh_sync_logos_repos',
    'gh_sync_waku_repos',
    'gh_sync_codex_repos',
    'disc_logos',
    'disc_vac',
    'disc_status',
    'load_hasura_logos_org_map'
]

@dag(
    'logos_org_map', 
    default_args=ARGS, 
    # Run  every 4 hours
    schedule_interval='0 */4  * * * '
)
def logos_org_map_sync():
    connections_id=fetch_airbyte_connections_tg(airbyte_connections)

    # Trigger Airbyte fetch Data from Github
    gh_sync_vac_repos = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_vac',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['gh_sync_vac_repos'],
        asynchronous=False,
        wait_seconds=3
    )
    gh_sync_logos_repos = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_logos',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['gh_sync_logos_repos'],
        asynchronous=False,
        wait_seconds=3
    )
    gh_sync_waku_repos=  AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_waku',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['gh_sync_waku_repos'],
        asynchronous=False,
        wait_seconds=3
    )
    gh_sync_codex_repos = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_codex',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['gh_sync_codex_repos'],
        asynchronous=False,
        wait_seconds=3
    )
    # We don't call gh_sync_status_sync because data are already sync with github_website_sync.py DAG

    # Launch DBT transformation on the data previously fetched
    dbt_run = BashOperator(
        task_id='dbt_run_models_projects',
        bash_command='dbt run --profiles-dir /dbt --project-dir /dbt/dbt-models/ --select projects'
    )
    # Trigger Airbyte Sync from main database to Hasura
    load_hasura = AirbyteTriggerSyncOperator(
        task_id='airbyte_sync_hasura',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['load_hasura_logos_org_map'],
        asynchronous=False,
        wait_seconds=3
    )

    connections_id >> [gh_sync_vac_repos, gh_sync_waku_repos, gh_sync_logos_repos, gh_sync_codex_repos] >> dbt_run >> load_hasura

logos_org_map_sync()
