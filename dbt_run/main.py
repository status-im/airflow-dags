import sys
import logging as LOG
from os import path
from datetime import datetime, timedelta
import logging
import sys

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash_operator import BashOperator
from docker.types import Mount

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

with DAG('dbt_execution', default_args=ARGS, schedule_interval=None, catchup=False) as dag:
    task_test =  DockerOperator(
	    task_id = 'dbt_test',
    	image = 'dbt:latest',
	    network_mode='host',
    	mounts=[
                    Mount(source="/docker/dbt-core/project/status-im/dbt-models", target="/usr/app", type="bind"), 
                    Mount(source="/docker/dbt-core/profile", target="/root/.dbt",type="bind")
                ],
    	command='test',
	    docker_url="unix://var/run/docker.sock",
    	auto_remove=False,
        tty=True,
    )
    task_run =  DockerOperator(
	    task_id = 'dbt_run',
    	image = 'dbt:latest',
	    network_mode='host',
  		mounts=[
                    Mount(source="/docker/dbt-core/project/status-im/dbt-models", target="/usr/app", type="bind"), 
                    Mount(source="/docker/dbt-core/profile", target="/root/.dbt",type="bind")
              ],
        command='run',
	    docker_url="unix://var/run/docker.sock",
    	auto_remove=False,
        tty=True,
    )
    task_test >> task_run

