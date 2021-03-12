from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['jakub@status.im'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'example_task_decorator',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    tags=['example'],
) as dag2:
    task_start = DummyOperator(task_id='task_start')
    task_mid_1 = DummyOperator(task_id='task_mid_1')
    task_mid_2 = DummyOperator(task_id='task_mid_2')
    task_mid_3 = DummyOperator(task_id='task_mid_3')
    task_end = DummyOperator(task_id='task_end')
    # Define dependencies
    task_start >> [task_mid_1, task_mid_2, task_mid_3] >> task_end
