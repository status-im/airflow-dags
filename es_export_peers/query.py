import hashlib
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook


def remove_prefix(text, prefix):
    return text[text.startswith(prefix) and len(prefix):]

def hash_string(text):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

class Peer:

    def __init__(self, date, peer, count):
        self.date = date
        self.peer = peer
        self.count = count

    def to_tuple(self):
        return (self.date, self.peer, self.count)

class ESQueryPeers():
    def __init__(self, conn_id='es_logs_cluster'):
        self.hook = ElasticsearchHook(elasticsearch_conn_id=conn_id)
        self.conn = self.hook.get_conn()
        self.client = self.conn.es

    def get_indices(self, pattern='logstash-*'):
        return self.client.indices.get(index=pattern).keys()

    def get_peers(self, index, field, fleet, program, max_query=100000):
        body = {
            'size': 10, # Don't return actual values
            'query': {
                'bool': {
                    'filter': [
                        { 'term': { 'fleet': fleet } },
                        { 'term': { 'program': program } },
                    ],
                },
            },
            'aggs': {
                'peers': {
                    'terms': {
                        'field': field,
                        'size': max_query,
                    },
                }, 
            },
        }
        # Query
        resp = self.client.search(index=index, body=body)
        aggs = resp.get('aggregations')

        # Collect results as list of dicts
        rval = []
        for bucket in aggs['peers']['buckets']:
            rval.append(Peer(
                date = remove_prefix(index, 'logstash-'),
                peer = hash_string(bucket['key']),
                count = bucket['doc_count']
            ))

        return rval
