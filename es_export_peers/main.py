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

dag = DAG(
    'es_export_peers',
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily'
)


def export_peers(index_pattern, field, fleet, program):
    # Uncomment to run with `airflow test` and debug.
    #from IPython import embed; embed()
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
    for index in esq.get_indices(index_pattern):
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
            field=field,
            fleet=fleet,
            program=program,
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
    op_kwargs={
        'index_pattern': 'logstash-202*',
        'field': 'peer_id',
        'fleet': 'eth.prod',
        'program': 'docker/statusd-whisper-node',
    },
    dag=dag
)
