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
from airflow.operators.bash_operator import BashOperator

from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

"""
DAG to sync data from github to the Website API 
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

AIRB_JOB_GITHUB_ID="d1b8879d-c16e-4029-bf72-182b9db0df4b"
AIRB_JOB_HASURA_ID="ea19a8d9-d396-4c0b-888d-6c4267c3a977"

with DAG('github_website_sync', 
            default_args=ARGS, 
            schedule_interval="@hourly", 
            catchup=False) as dag:


    get_workflow_id = SimpleHttpOperator(
        task_id='get_workflow_id',
        http_conn_id='airbyte_conn_example',
        endpoint='/api/v1/workspaces/list',
        method="POST",
        headers={"Content-type": "application/json"},
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
        headers={"Content-type": "application/json"},
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
        github_conn=list(filter(lambda x: x['name'] == 'Github-status-im-fetch', output))
        hasura_conn=list(filter(lambda x: x['name'] == 'Load Hasura', output))
        print(f'Github_conId={github_conn} \n Hasura_conn_Id={hasura_conn}')
        print(f'Github_conId={github_conn[0]["connectionId"]} \n Hasura_conn_Id={hasura_conn[0]["connectionId"]}')
        return { 
                "github": f"{github_conn[0]['connectionId']}",
                "hasura": f"{hasura_conn[0]['connectionId']}" 
               }
        
    connections_id = extract_conn_id(get_connections.output)
    
    connections_id.doc_md = """\
            ## Extract Connections ID
            
            This task extract the connection ID from the connection `Github-status-im-fetch` and `Load Hasura` 
            """

# Trigger Airbyte fetch Data from Github
    airbyte_fetch_github = AirbyteTriggerSyncOperator(
        task_id='airbyte_fetch_github',
        airbyte_conn_id='airbyte_conn_example',
        connection_id=connections_id['github'],
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )
    airbyte_fetch_github.doc_md = """\
            ## Airbyte Fetch in Github
            
            This task fetch data from the repo in Github status-im.
            It calls Airbyte connection `Github-status-im-fetch`
            """


# Launch DBT transformation on the data previously fetched
    dbt_transform = BashOperator(
            task_id='dbt_postgres_run',
            bash_command='dbt run --profiles-dir /dbt --project-dir /dbt/status-im/dbt-models/models_postgres'
    )
    dbt_transform.doc_md = """\
            ## DBT transform
            
            This task launch a dbt transformation on the data fetch from Github
            """

# Trigger Airbyte Sync from main database to Hasura
    airbyte_sync_hasura = AirbyteTriggerSyncOperator(
        task_id='airbyte_sync_hasura',
        airbyte_conn_id='airbyte_conn_example',
        connection_id=connections_id['hasura'],
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )
    airbyte_sync_hasura.doc_md = """\
            ## Airbyte Sync Hasura
            
            This task push the transformed data to Hasura DB.
            It calls Airbyte connection named `Load Hasura`
            """

    get_workflow_id >> get_connections >> connections_id >> airbyte_fetch_github >> dbt_transform >>  airbyte_sync_hasura

dag.doc_md = __doc__
