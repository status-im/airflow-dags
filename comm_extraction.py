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
    'twitter-nomos',
    'twitter-codex',
    'twitter-logos',
    'twitter-status',
    'twitter-nimbus',
    'twitter-waku',
    'twitter-vac',
    'twitter-keycard',
    'twitter-ift',
    'twitter-operator',
]


@task(task_id="wait_for_api")
def wait_for_api():
    # Twitter API limit number of call each 15 min
    # https://developer.twitter.com/en/docs/twitter-api/tweets/lookup/api-reference/get-tweets#tab1
    time.sleep(900)

# Task group to add a waiting time for the API to avoid reaching the API rate limit
@task_group(group_id='fetch_twitter_info')
def fetch_twitter_info(connection_id, airb_conn_name):
    twitter_fetch = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_twittter_'+airb_conn_name,
        airbyte_conn_id='airbyte_conn',
        connection_id=connection_id,
        asynchronous=False,
        wait_seconds=3,
        trigger_rule="all_done"
    )
    twitter_fetch >> wait_for_api()

@dag(
    'comm_extraction',
    default_args=ARGS,
    schedule_interval='0 */24 * * * '
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

    #twitter_acid_info = fetch_twitter_info(connections_id['twitter-acid'], 'acid_info')

    twitter_nomos_tech = fetch_twitter_info(connections_id['twitter-nomos'], 'nomos')

    twitter_codex = fetch_twitter_info(connections_id['twitter-codex'], 'codex')

    twitter_logos = fetch_twitter_info(connections_id['twitter-logos'], 'logos')

    twitter_status = fetch_twitter_info(connections_id['twitter-status'], 'status')

    twitter_nimbus = fetch_twitter_info(connections_id['twitter-nimbus'], 'nimbus')

    twitter_waku = fetch_twitter_info(connections_id['twitter-waku'], 'waku')

    twitter_vac = fetch_twitter_info(connections_id['twitter-vac'], 'vac')

    twitter_keycard = fetch_twitter_info(connections_id['twitter-keycard'], 'keycard')

    twitter_ift = fetch_twitter_info(connections_id['twitter-ift'], 'ift')

    twitter_operator = fetch_twitter_info(connections_id['twitter-operator'], 'operator')

    dbt_run_socials = BashOperator(
        task_id='dbt_run_models_social',
        bash_command='dbt run --profiles-dir /dbt --project-dir /dbt/dbt-models/ --select social'
    )
    dbt_run_twitter = BashOperator(
        task_id='dbt_run_models_twitter',
        bash_command='dbt run --profiles-dir /dbt --project-dir /dbt/dbt-models/ --select twitter'
    )

    # Twitter connections have to be sequentially run to avoid API Rate Limits
    connections_id >> [discord_fetcher, simplecast_fetch] >> twitter_nomos_tech >> twitter_codex >> twitter_logos >> twitter_waku >> twitter_nimbus >> twitter_vac >> twitter_keycard >> twitter_operator >> twitter_ift >> dbt_run_socials >> dbt_run_twitter


comm_extraction()
