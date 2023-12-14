import sys
from os import path
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# HACK: Fix for loading relative modules.
sys.path.append(path.dirname(path.realpath(__file__)))
from tasks.airbyte import fetch_airbyte_connections_tg
from providers.airbyte.operator import AirbyteTriggerSyncOperator

"""
DAG to fetch data from Spiff workflow
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

airbyte_connections=['extract_spiff_backend_mod_prod', 'extract_spiff_connector_mod_prod']

@dag('spiff_extraction', 
            default_args=ARGS, 
            schedule_interval='30 */6 * * *')
def spiff_extraction():
    connections_id=fetch_airbyte_connections_tg(airbyte_connections)

    # Trigger Airbyte sync for Spiff Backend DB
    AirbyteTriggerSyncOperator(
        task_id='airbyte_sync_spiff_backend',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['extract_spiff_backend'],
        asynchronous=False,
        wait_seconds=3
    )

    AirbyteTriggerSyncOperator(
        task_id='airbyte_sync_spiff_connector',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['extract_spiff_connector'],
        asynchronous=False,
        wait_seconds=3
    )

spiff_extraction()
