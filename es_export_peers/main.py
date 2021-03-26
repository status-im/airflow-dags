import sys
import requests
import logging as LOG
from os import path
from datetime import datetime
from optparse import OptionParser

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# HACK: Fix for loading relative modules.
sys.path.append(path.dirname(path.realpath(__file__)))


from query import ESQueryPeers
from postgres import PGDatabase

# These are applied to all Operators via `kwargs["dag"]`:
DEFAULT_ARGS = {
    'owner': 'jakubgs',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 24),
    'email': ['jakub@status.im'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
}

# These are passed to all Operators via `kwargs['dag_run'].conf`:
DEFAULT_PARAMS = {
    'index_pattern': 'logstash-202*',
    'field_name': 'peer_id',
    'fleet_name': 'eth.prod',
    'program': 'docker/statusd-whisper-node',
}

# Main definition of the DAG, needs to be global.
dag = DAG(
    'es_export_peers',
    schedule_interval='@daily',
    default_args=DEFAULT_ARGS,
    params=DEFAULT_PARAMS
)


def export_peers(**kwargs):
    LOG.info(kwargs)
    # This passes arguments given via Web UI when triggering a DAG.
    conf = kwargs['dag_run'].conf

    esq = ESQueryPeers(
        Variable.get('es_log_cluster_addr'),
        Variable.get('es_log_cluster_port'),
    )
    psg = PGDatabase(
        Variable.get('citus_db_name'),
        Variable.get('citus_db_user'),
        Variable.get('citus_db_pass'),
        Variable.get('citus_db_host'),
        Variable.get('citus_db_port')
    )

    days = psg.get_present_days()
    present_indices = [('logstash-%s' % d.replace('-', '.')) for d in days]

    LOG.info('Querying ES cluster for peers...')
    peers = []
    for index in esq.get_indices(conf['index_pattern']):
        # skip already injected indices
        if index in present_indices:
            LOG.debug('Skipping existing index: %s', index)
            continue
        # skip current day as it's incomplete
        if index == datetime.now().strftime('logstash-%Y.%m.%d'):
            LOG.debug('Skipping incomplete current day.')
            continue
        LOG.info('Index: %s', index)
        rval = esq.get_peers(
            index=index,
            field=conf['field_name'],
            fleet=conf['fleet_name'],
            program=conf['program'],
        )
        if len(rval) == 0:
            LOG.warning('No entries found!')
        LOG.debug('Found: %s', len(rval))
        peers.extend(rval)

    if len(peers) == 0:
        LOG.info('Nothing to insert into database.')
        return

    LOG.info('Injecting peers data into database...')
    psg.inject_peers(peers)

export_peers = PythonOperator(
    task_id='export_peers',
    python_callable=export_peers,
    dag=dag
)
