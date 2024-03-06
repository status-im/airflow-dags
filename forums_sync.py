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
    'disc_logos',
    'disc_vac',
    'disc_status'
]

@dag(
    'forums_sync', 
    default_args=ARGS, 
    # Run  every 4 hours
    schedule_interval='0 */4  * * * '
)
def forums_sync():
    connections_id=fetch_airbyte_connections_tg(airbyte_connections)

    # Trigger Airbyte fetch Data from Discourse 
    disc_logos = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_disc_logos',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['disc_logos'],
        asynchronous=False,
        wait_seconds=3
    )
    disc_status = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_disc_status',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['disc_status'],
        asynchronous=False,
        wait_seconds=3
    )
    disc_vac = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_disc_vac',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['disc_vac'],
        asynchronous=False,
        wait_seconds=3
    )

    connections_id >> [disc_logos, disc_vac, disc_status] 

forums_sync()
