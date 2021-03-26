import logging as LOG
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator

# This is similar to task_decorator.py but generates tasks dynamically.

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['jakub@status.im'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'example_task_generator',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    tags=['example'],
) as dag:
    task_start = DummyOperator(task_id='task_start')
    task_end = DummyOperator(task_id='task_end')

    generated_tasks = []
    for i in range(3):
        task = DummyOperator(task_id='task_mid_%s' % (i+1))
        generated_tasks.append(task)

    # Define dependencies
    task_start >> generated_tasks >> task_end
