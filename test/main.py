import sys
import logging as LOG
from os import path
from datetime import datetime, timedelta
import logging
import sys
import  json
import airflow.models.taskinstance
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.bash_operator import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator

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

def test_python(test):
    print(f"value: {test}")

with DAG('test', default_args=ARGS, schedule_interval=None, catchup=False) as dag:

    get_workflow_id = SimpleHttpOperator(
        task_id='get_workflow_id',
        http_conn_id='airbyte_conn_example',
        endpoint='/api/v1/workspaces/list',
        method="POST",
        headers={"Content-type": "application/json"},
        response_filter=lambda response: response.json()["workspaces"][0]["workspaceId"],
    )
    
    get_connections_id = SimpleHttpOperator(
        task_id='get_connections_id',
        http_conn_id='airbyte_conn_example',
        endpoint='/api/v1/connections/list',
        method="POST",
        headers={"Content-type": "application/json"},
        data=json.dumps(
            {"workspaceId": f"{get_workflow_id.output}"}
            ),
        response_filter= lambda response: response.json()["connections"]
    )
    
    @dag.task(task_id="extracts_conn_id")
    def extract_conn_id(output):
        github_conn=list(filter(lambda x: x['name'] == 'Github-status-im-fetch', output))
        hasura_conn=list(filter(lambda x: x['name'] == 'Load Hasura', output))
        print(f'Github_conId={github_conn} \n Hasura_conn_Id={hasura_conn}')
        print(f'Github_conId={github_conn[0]["connectionId"]} \n Hasura_conn_Id={hasura_conn[0]["connectionId"]}')
        return { "github": f"{github_conn[0]['connectionId']}", "hasura": f"{hasura_conn[0]['connectionId']}" }
        
        #        print [obj for obj in output["connections"]  if (obj["name"] in ["Load Hasura","Github-status-im-fetch"]
    extract_connections_id = extract_conn_id(get_connections_id.output)

    get_workflow_id >> get_connections_id >> extract_connections_id
