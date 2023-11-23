# Description

This repo contains implementations of Airflow workflows and tasks called respectively [DAGs](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#dags) and [Operators](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#operators).

* DAGs - Direct Acyclic Graphs - Python scripts defining workflows in a way that reflects their relationships.
* Operators - Python functions which define the individual tasks that are executed as part of a DAG run.

To learn how to write DAGs and Operators read about [core concepts](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#concepts) and follow the [official tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html).

# DAG

This repository contains: 

* `website_sync`: DAG to launch the Airbyte jobs for the status-website charts.
* `spiff_sync`: DAG to synchronize Spiff workflows data
* `dbt`: DAG to run all the dbt models,
* `gh_sync`: DAG to synchronize data from repository (logos, waku, codex)

> The DBT models run in some DAG are stored in [`dbt-models`](https://github.com/status-im/dbt-models).

# Continuous Integration

Changes pushed to `master` are automatically fetched to our Airflow instance by the [`airflow-webhook`](https://github.com/status-im/infra-bi/tree/master/ansible/roles/airflow-webhook) service.

# Branches

This repos has 3 working branches:

* `prod`: used by https://airflow.bi.status.im.
* `test`: used by https://airflow.test.bi.status.im to test DAGs modification.
* `example`: contains examples of DAGs

# Infrastructure

All Airflow infrastructure is managed in the [infra-bi](https://github.com/status-im/infra-bi) repository.
