import sys
import json
import logging as LOG
from os import path
from datetime import datetime, timedelta
import logging
import sys

from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import SimpleHttpOperator



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


with DAG('spiff_sync', 
            default_args=ARGS, 
            schedule_interval='0 /6 * * *',
            catchup=False) as dag:

    get_workflow_id = SimpleHttpOperator(
        task_id='get_workflow_id',
        http_conn_id='airbyte_conn',
        endpoint='/api/v1/workspaces/list',
        method="POST",
        headers={"Content-type": "application/json", "timeout": "1200"},
        response_filter=lambda response: response.json()["workspaces"][0]["workspaceId"],
    )
    get_workflow_id.doc_md = """\
            ## Getting the workflow ID
            
            This task call Airbyte API to get the workflow ID.
            It is necessairy to get the connections ID for triggering the jobs in Airbyte.
            """

    get_connections = SimpleHttpOperator(
        task_id='get_connections_id',
        http_conn_id='airbyte_conn_example',
        endpoint='/api/v1/connections/list',
        method="POST",
        headers={"Content-type": "application/json", "timeout": "1200"},
        data=json.dumps(
            {"workspaceId": f"{get_workflow_id.output}"}
            ),
        response_filter= lambda response: response.json()["connections"]
    )

    get_connections.doc_md = """\
            ## Getting Connctions
            
            This task call Airbyte API to get the connections ID.
            It is necessairy to get the connections ID for triggering the jobs in Airbyte.
            """


    @dag.task(task_id="extracts_conn_id", multiple_outputs=True)
    def extract_conn_id(output):
        logging.info('Connection ID %s' % output)
        backend_conn=list(filter(lambda x: x['name'] == 'extract_spiff_backend_mod_prod', output))
        connector_conn=list(filter(lambda x: x['name'] == 'extract_spiff_connector_mod_prod', output))
        return { 
                "extract_spiff_backend_mod_prod": f"{backend_conn[0]['connectionId']}",
                "extract_spiff_connector_mod_prod": f"{connector_conn[0]['connectionId']}" 
               }
        
    connections_id = extract_conn_id(get_connections.output)
    
# Trigger Airbyte sync for Spiff Backend DB
    airbyte_sync_spiff_backend = SimpleHttpOperator(
        task_id='airbyte_sync_spiff_backend',
        http_conn_id='airbyte_conn',
        endpoint='/api/v1/connections/sync',
        method="POST",
        headers={"Content-type": "application/json", "timeout": "1200"},
        data=json.dumps(
            {"connectionId": f"{connections_id['extract_spiff_backend_mod_prod']}"}
            )
    )

# Trigger Airbyte Sync for Spiff Connector DB
    airbyte_sync_spiff_connector = SimpleHttpOperator(
        task_id='airbyte_sync_spiff_connector',
        http_conn_id='airbyte_conn',
        endpoint='/api/v1/connections/sync',
        method="POST",
        headers={"Content-type": "application/json", "timeout": "1200"},
        data=json.dumps(
            {"connectionId": f"{connections_id['extract_spiff_connector_mod_prod']}"}
            )
    )

    get_workflow_id >> get_connections >> connections_id >> airbyte_sync_spiff_backend >>  airbyte_sync_spiff_connector

dag.doc_md = __doc__
