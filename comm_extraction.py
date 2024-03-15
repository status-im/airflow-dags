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
    'simplecast_fetch'
]

@dag(
    'comm_extraction', 
    default_args=ARGS, 
    # Run  every 4 hours
    schedule_interval='0 */24  * * * '
)
def forums_sync():
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
    
    connections_id >> [discord_fetcher, simplecast_fetch] 

forums_sync()
