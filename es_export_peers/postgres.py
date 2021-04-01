from airflow.providers.postgres.hooks.postgres import PostgresHook

class PGDatabase:
    _SCHEMA = """
       CREATE TABLE IF NOT EXISTS peers (date date, peer varchar(64), count int);
    """

    def __init__(self, conn_id='citus_db_peers'):
        self.hook = PostgresHook(postgres_conn_id=conn_id)
        self.db = self.hook.get_conn()
        self.c = self.db.cursor()
        self._create_schema()

    def _create_schema(self):
        self.c.execute(self._SCHEMA)
        self.db.commit()

    def get_present_days(self):
        self.c.execute('SELECT DISTINCT date FROM peers ORDER BY date;')
        return [d[0].strftime('%Y-%m-%d') for d in self.c.fetchall()]

    def inject_peers(self, peers):
        args = ','.join(
            self.c.mogrify('(%s,%s,%s)', peer.to_tuple()).decode('utf-8')
            for peer in peers
        )
        rval = self.c.execute(
            'INSERT INTO peers(date, peer, count) VALUES {}'.format(args)
        )
        self.db.commit()
