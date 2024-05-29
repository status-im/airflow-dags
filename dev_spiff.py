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
DAG to sync data for mod prod spiff environment
"""
logging.basicConfig(stream=sys.stdout, level=logging.info)

ARGS = { 
    'owner': 'lanaya',
    'depends_on_past': False,
    'start_date': datetime(2023,6,1),
    'email': ['lalo@status.im'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
}


airbyte_connections = [
    'extract_spiff_backend_app_dev'
]

@dag('app-dev-spiff-data-sync', schedule_interval='0 0/12 * * *', default_args=ARGS)
def app_dev_spiff_dashboard_sync():
    
    connections_id=fetch_airbyte_connections_tg(airbyte_connections)

    fetch_bank_spiff_backend_dev_data = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_backend_dev',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['extract_spiff_backend_app_dev'],
        asynchronous=False,
        wait_seconds=3
    )

    dbt_run_prod_spiff = BashOperator(
        task_id='dbt_run_models_dev_spiff',
        bash_command='dbt run --profiles-dir /dbt --project-dir /dbt/dbt-models/ --select dev_spiff'
    )

    connections_id >> fetch_bank_spiff_backend_dev_data >> dbt_run_prod_spiff 

app_dev_spiff_dashboard_sync()
