# Description

This Python script queries for unique Status peers and pushes them to an SQL database.

# Details

The script queries an ElasticSearch endpoint for `logstash-*` indices and aggregates counts of instances of log messages with set `peer_id` field.

This data is pushed to a PostgreSQL database in the following format:
```
peers=> \d peers;
                       Table "public.peers"
┌────────┬───────────────────────┬───────────┬──────────┬─────────┐
│ Column │         Type          │ Collation │ Nullable │ Default │
├────────┼───────────────────────┼───────────┼──────────┼─────────┤
│ date   │ date                  │           │          │         │
│ peer   │ character varying(64) │           │          │         │
│ count  │ integer               │           │          │         │
└────────┴───────────────────────┴───────────┴──────────┴─────────┘
```

# Usage

The DAG assumes existence of several Airflow variables:
```
es_log_cluster_addr
es_log_cluster_port
citus_db_name
citus_db_user
citus_db_pass
citus_db_host
citus_db_port
```

# Example

```
peers=> select * from peers limit 3;
┌────────────┬──────────────────────────────────────────────────────────────────┬───────┐
│    date    │                               peer                               │ count │
├────────────┼──────────────────────────────────────────────────────────────────┼───────┤
│ 2020-06-01 │ a18d4417b1d2fbddd7f9474250f703ba20472be5e1131bc09e35e9b18c1a5bf7 │  1300 │
│ 2020-06-01 │ 7dba96249159cef53fbb5ec010c2d7799fec7dcaf8b1d9754559ce9fbd463328 │   652 │
│ 2020-06-01 │ 3a13adfa4799f9505c83fab18d49a47f6de09344db3d96e18678c5d3c92f717e │   632 │
└────────────┴──────────────────────────────────────────────────────────────────┴───────┘
(3 rows)
```
