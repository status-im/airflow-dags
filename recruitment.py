import sys
import time
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
DAG to sync data for Recruitment task
"""

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

ARGS = {
    'owner': 'apentori',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 11),
    'email': ['alexis@status.im'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
}

airbyte_connections=[
        'recruitment-waku-part_1',
        'recruitment-waku-part_2',
        'recruitment-waku-part_3',
        'recruitment-waku-part_4',
        'recruitment-waku-part_5',
        ]


@task(task_id="wait_for_api")
def wait_for_api():
    # Twitter API limit number of call each 15 min
    # https://developer.twitter.com/en/docs/twitter-api/tweets/lookup/api-reference/get-tweets#tab1
    time.sleep(900)


@dag('recruitment',
            default_args=ARGS,
            schedule_interval='0 3 * * *')
def recruitment():
    connections_id=fetch_airbyte_connections_tg(airbyte_connections)

    part_1 = AirbyteTriggerSyncOperator(
        task_id='gh_sync_recruit_1',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['recruitment-waku-part_1'],
        asynchronous=False,
        wait_seconds=3
    )
    part_2 = AirbyteTriggerSyncOperator(
        task_id='gh_sync_recruit_2',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['recruitment-waku-part_2'],
        asynchronous=False,
        wait_seconds=3
    )
    part_3 = AirbyteTriggerSyncOperator(
        task_id='gh_sync_recruit_3',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['recruitment-waku-part_3'],
        asynchronous=False,
        wait_seconds=3
    )
    part_4 = AirbyteTriggerSyncOperator(
        task_id='gh_sync_recruit_4',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['recruitment-waku-part_4'],
        asynchronous=False,
        wait_seconds=3
    )
    part_5 = AirbyteTriggerSyncOperator(
        task_id='gh_sync_recruit_5',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['recruitment-waku-part_5'],
        asynchronous=False,
        wait_seconds=3
    )

    connections_id >> part_4 >> wait_for_api() >> part_3 >> wait_for_api() >> part_2 >> wait_for_api() >> part_1 >> wait_for_api() >> part_5

recruitment()
