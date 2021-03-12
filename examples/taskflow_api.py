# Source: https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#taskflow-api
import json
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.email import EmailOperator

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['jakub@status.im'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'example_taskflow_api',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval=timedelta(days=1),
    tags=['example'],
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html#catchup
    catchup=False
) as dag:
  # Assumes existence of http_default Connection which calls https://httpbin.org/
  get_ip = SimpleHttpOperator(
      task_id='get_ip',
      http_conn_id='http_default',
      endpoint='get',
      method='GET'
  )

  @dag.task(multiple_outputs=True)
  def prepare_email(raw_json: str):
    external_ip = json.loads(raw_json)['origin']
    return {
      'subject':f'Server connected from {external_ip}',
      'body': f'Seems like today your server executing Airflow is connected from the external IP {external_ip}<br>'
    }

  email_info = prepare_email(get_ip.output)

  send_email = EmailOperator(
      task_id='send_email',
      to='jakub@status.im',
      subject=email_info['subject'],
      html_content=email_info['body']
  )
