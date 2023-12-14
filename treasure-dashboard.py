import sys
import logging
from os import path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param

from airflow.decorators import dag

# HACK: Fix for loading relative modules.
sys.path.append(path.dirname(path.realpath(__file__)))
from tasks.airbyte import fetch_airbyte_connections_tg

from providers.airbyte.operator import AirbyteTriggerSyncOperator

"""
DAG to sync data to create the Treasure Dashboard
"""
logging.basicConfig(stream=sys.stdout, level=logging.info)

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


airbyte_connections=['blockchain-wallet-sync']

@dag('treasure-dashboard-sync', schedule_interval='@daily', default_args=ARGS)
def treasure_dashboard_sync():
    connections_id=fetch_airbyte_connections_tg(airbyte_connections)
    AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_blockchain_wallet',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['blockchain-wallet-sync'],
        asynchronous=False,
        wait_seconds=3
    )

treasure_dashboard_sync()
