import sys
import json
from os import path
from datetime import datetime, timedelta
import logging
import time
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
DAG to sync data from the Discourse forums used accros the org 
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
    'discord_fetcher',
    'simplecast_fetch',
    'com_twitter_nomos_tech',
    'com_twitter_ac1d_info',
    'com_twitter_codex',
    'com_twitter_ethstatus',
    'com_twitter_logos',
    'com_twitter_nimbus',
    'com_twitter_waku'
]


@task(task_id="wait_for_api")
def wait_for_api():
    # Twitter API limit number of call each 15 min
    # https://developer.twitter.com/en/docs/twitter-api/tweets/lookup/api-reference/get-tweets#tab1
    time.sleep(900)


@dag(
    'comm_extraction',
    default_args=ARGS,
    # Run  every 4 hours
    schedule_interval='0 */24  * * * '
)
def comm_extraction():
    connections_id=fetch_airbyte_connections_tg(airbyte_connections)

    # Trigger Airbyte fetch Data from Discourse
    discord_fetcher = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_discord',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['discord_fetcher'],
        asynchronous=False,
        wait_seconds=3
    )
    simplecast_fetch = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_simplecast',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['simplecast_fetch'],
        asynchronous=False,
        wait_seconds=3
    )
    twitter_acid_info = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_twitter_acid_info',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['com_twitter_ac1d_info'],
        asynchronous=False,
        wait_seconds=3
    )
    twitter_nomos_tech = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_twitter_nomos_tech',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['com_twitter_nomos_tech'],
        asynchronous=False,
        wait_seconds=3
    )
    twitter_codex = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_twitter_codex',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['com_twitter_codex'],
        asynchronous=False,
        wait_seconds=3
    )
    twitter_logos = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_twitter_logos',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['com_twitter_logos'],
        asynchronous=False,
        wait_seconds=3
    )
    twitter_ethstatus = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_twitter_ethstatus',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['com_twitter_ethstatus'],
        asynchronous=False,
        wait_seconds=3
    )
    twitter_nimbus = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_twitter_nimbus',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['com_twitter_nimbus'],
        asynchronous=False,
        wait_seconds=3
    )
    twitter_waku = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_twitter_waku',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['com_twitter_waku'],
        asynchronous=False,
        wait_seconds=3
    )

    dbt_run_socials = BashOperator(
        task_id='dbt_run_models_social',
        bash_command='dbt run --profiles-dir /dbt --project-dir /dbt/dbt-models/ --select social'
    )

    
    connections_id >> [discord_fetcher, simplecast_fetch] >> twitter_acid_info >> wait_for_api() >> twitter_nomos_tech >> wait_for_api() >> twitter_codex >> wait_for_api() >> twitter_ethstatus >> wait_for_api() >> twitter_logos >> wait_for_api() >> twitter_waku >> wait_for_api() >> twitter_nimbus >> wait_for_api() >> dbt_run_socials

comm_extraction()
