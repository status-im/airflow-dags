import sys
import json
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow.providers.http.operators.http import SimpleHttpOperator

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

@task(task_id="extracts_conn_id", multiple_outputs=True)
def extract_conn_id(output, connections):
    ids={}
    for c in connections:
        id=list(filter(lambda x:x['name'] == c, output))[0]['connectionId']
        logging.info('%s has the id %s', c, id)
        ids[c]=id
    return ids
 

@task_group(group_id='my_task_group')
def fetch_airbyte_connections_tg(connections):
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

    connections_id = extract_conn_id(
            get_connections.output, 
            connections
    )

    get_workflow_id >> get_connections >> connections_id

    return connections_id
