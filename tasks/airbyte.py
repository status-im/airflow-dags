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

@task(task_id="create_payload_soure_config")
def create_payload_soure_config(config, source_name, source_id):
    data={
        "sourceId": f"{source_id}",
        "connectionConfiguration": {
            "wallets": config
        },
        "name": f"{source_name}"
    }
    Variable.set("wallet_config", json.dumps(data));

@task_group(group_id='fetch_airbyte_connections_tg')
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
        data=json.dumps({
            "workspaceId": f"{get_workflow_id.output}"
        }),
        response_filter= lambda response: response.json()["connections"]
    )

    connections_id = extract_conn_id(
            get_connections.output, 
            connections
    )

    get_workflow_id >> get_connections >> connections_id

    return connections_id


@task_group(group_id='update_airbyte_source_config_tg')
def update_airbyte_source_config_tg(source_name, config):

    get_sources_id = SimpleHttpOperator(
        task_id='get_sources_id',
        http_conn_id='airbyte_conn',
        endpoint='/api/v1/sources/search',
        method="POST",
        headers={"Content-type": "application/json", "timeout": "1200"},
        data=json.dumps({
             "name": f"{source_name}"
        }),
        response_filter=lambda response: response.json()["sources"][0]['sourceId']
    )
    
    source_config_payload = create_payload_soure_config(config, source_name, get_sources_id.output)

    update_source_config = SimpleHttpOperator(
        task_id='update_source_config',
        http_conn_id='airbyte_conn',
        endpoint='/api/v1/sources/update',
        method="POST",
        headers={"Content-type": "application/json", "timeout": "1200"},
        data=Variable.get('wallet_config')
    )
    get_sources_id >> source_config_payload >> update_source_config
