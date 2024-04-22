import sys
import logging
from os import path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param

from airflow.decorators import dag
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.bash_operator import BashOperator

# HACK: Fix for loading relative modules.
sys.path.append(path.dirname(path.realpath(__file__)))
from tasks.airbyte import fetch_airbyte_connections_tg, update_airbyte_source_config_tg
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


airbyte_connections = [
    'treasure-dsh-fetch-wallets-balance', 
    'treasure-dsh-fetch-bank-balance',
    'treasure-dsh-fetch-xecurrencies',
    'treasure-dsh-fetch-coingecko'
]

@dag('treasure-dashboard-sync', schedule_interval='0 0/6 * * *', default_args=ARGS)
def treasure_dashboard_sync():
    
    connections_id=fetch_airbyte_connections_tg(airbyte_connections)

    wallets_config = SimpleHttpOperator(
        task_id='get_wallet_config',
        http_conn_id='config_server_conn',
        endpoint='/api/wallets',
        method="GET",
        headers={"API-Key": Variable.get('CONFIG_SERVER_API_KEY')},
        response_filter=lambda response: response.json(),
    )

    update_airbyte_config = update_airbyte_source_config_tg(
        "fin_dsh_wallet_fetcher", 
        wallets_config.output
    )

    fetch_wallet_data = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_blockchain_wallet',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['treasure-dsh-fetch-wallets-balance'],
        asynchronous=False,
        wait_seconds=3
    )

    fetch_xe_data = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_xe_data',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['treasure-dsh-fetch-xecurrencies'],
        asynchronous=False,
        wait_seconds=3
    )

    fetch_bank_balance_data = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_bank_balance',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['treasure-dsh-fetch-bank-balance'],
        asynchronous=False,
        wait_seconds=3
    )

    fetch_coingecko_data = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_coingecko_data',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['treasure-dsh-fetch-coingecko'],
        asynchronous=False,
        wait_seconds=3
    )

    dbt_run_blockchain = BashOperator(
        task_id='dbt_run_models_blockchain',
        bash_command='dbt run --profiles-dir /dbt --project-dir /dbt/dbt-models/ --select blockchain'
    )

    dbt_run_finance = BashOperator(
        task_id='dbt_run_models_finance',
        bash_command='dbt run --profiles-dir /dbt --project-dir /dbt/dbt-models/ --select finance'
    )

    connections_id >> wallets_config >> update_airbyte_config >> fetch_wallet_data >> fetch_xe_data >> fetch_coingecko_data >> fetch_bank_balance_data >> dbt_run_blockchain >> dbt_run_finance 

treasure_dashboard_sync()
