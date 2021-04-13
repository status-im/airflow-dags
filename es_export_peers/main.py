import sys
import logging as LOG
from os import path
from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook

# HACK: Fix for loading relative modules.
sys.path.append(path.dirname(path.realpath(__file__)))

from query import ESQueryPeers
from postgres import PGDatabase

# These are applied to all Operators via `kwargs["dag"]`:
ARGS = {
    'owner': 'jakubgs',
    'depends_on_past': False,
    'start_date': datetime(2021, 4, 13),
    'email': ['jakub@status.im'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
}

# These are passed to all Operators via `get_current_context()['params']`:
PARAMS = {
    'index_pattern': 'logstash-202*',
    'field_name': 'peer_id',
    'fleet_name': 'eth.prod',
    'program': 'docker/statusd-whisper-node',
}

# ElasticSearch Logs Cluster
esq = ESQueryPeers(conn_id='es_logs_cluster')
# Citus PostgreSQL Database
psg = PGDatabase(conn_id='citus_db_peers')


@task
def query_indices(**kwargs):
    # This passes arguments given via Web UI when triggering a DAG.
    params = get_current_context()['params']

    days = psg.get_present_days()
    present_indices = [('logstash-%s' % d.replace('-', '.')) for d in days]

    LOG.info('Querying ES cluster for peers...')
    indices_to_query = []
    for index_name in esq.get_indices(params['index_pattern']):
        LOG.debug('Found Index: %s', index_name)

        # skip already injected indices
        if index_name in present_indices:
            LOG.debug('Skipping existing index: %s', index_name)
            continue
        # skip current day as it's incomplete
        if index_name == datetime.now().strftime('logstash-%Y.%m.%d'):
            LOG.warning('Skipping incomplete current day.')
            continue

        indices_to_query.append(index_name)

    return list(indices_to_query)

@task
def query_peers(indices: list):
    # This passes arguments given via Web UI when triggering a DAG.
    params = get_current_context()['params']

    peers = []
    for index_name in indices:
        rval = esq.get_peers(
            index=index_name,
            field=params['field_name'],
            fleet=params['fleet_name'],
            program=params['program'],
        )
        if len(rval) == 0:
            LOG.warning('%s - No entries found!', index_name)
            continue

        LOG.info('%s - Found: %s', index_name, len(rval))
        peers.extend(rval)

    return peers

@task
def inject_peers(peers: list):
    if len(peers) == 0:
        LOG.warning('Nothing to insert into database.')
        return

    LOG.info('Injecting peers data into database...')
    psg.inject_peers(peers)


# Main definition of the DAG, needs to be global.
@dag('es_export_peers', schedule_interval='@daily', default_args=ARGS, params=PARAMS)
def es_export_peers():
    inject_peers(query_peers(query_indices()))

dag = es_export_peers()
