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
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

"""
DAG to fetch data from the different repo organisation in GitHub
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

with DAG('gh_repos_sync',
         default_args=ARGS,
         schedule_interval='6 * * * *',
         catchup=False) as dag:

    get_workflow_id = SimpleHttpOperator(
        task_id='get_workflow_id',
        http_conn_id='airbyte_conn',
        endpoint='/api/v1/workspaces/list',
        method="POST",
        headers={"Content-type": "application/json", "timeout": "1200"},
        response_filter=lambda response: response.json()["workspaces"][0]["workspaceId"],
    )

    get_connections = SimpleHttpOperator(
        task_id='get_connections_id',
        http_conn_id='airbyte_conn',
        endpoint='/api/v1/connections/list',
        method="POST",
        headers={"Content-type": "application/json", "timeout": "1200"},
        data=json.dumps(
            {"workspaceId": f"{get_workflow_id.output}"}
            ),
        response_filter= lambda response: response.json()["connections"]
    )

    @dag.task(task_id="extracts_conn_id", multiple_outputs=True)
    def extract_conn_id(output):
        logging.info('Connection ID %s' % output)
        backend_conn=list(filter(lambda x: x['name'] == 'gh_sync_logos_repos', output))
        return { 
                "gh_logos": f"{backend_conn[0]['connectionId']}",
               }
        
    connections_id = extract_conn_id(get_connections.output)

    airbyte_fetch_logos = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_github',
        airbyte_conn_id='airbyte_conn',
        connection_id=connections_id['gh_logos'],
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    get_workflow_id >> get_connections >> connections_id >> airbyte_fetch_logos
