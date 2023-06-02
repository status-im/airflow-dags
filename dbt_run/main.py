import sys
import logging as LOG
from os import path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash_operator import BashOperator

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

with DAG('docker_dag', default_args=ARGS, schedule_interval="5 * * * *", catchup=False) as dag:
    t1=BashOperator(
    task_id='print_current_date',
    bash_command='date'
    )
    t2 = DockerOperator(
    task_id='docker_command',
    image='centos:latest',
    api_version='auto',
    auto_remove=True,
    command="/bin/sleep 30",
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge"
    )
    t3 =  DockerOperator(
	task_id = 'run_dbt',
	image = 'dbt:test',
	network_mode='host',
	volumes=['/docker/dbt-core/project/status-im/dbt-models:/usr/app', '/docker/dbt-core/profile:/root/.dbt'],
	command='run',
	docker_url="unix://var/run/docker.sock",
	auto_remove=False,
    )
    t1 >> t2  >> t3

