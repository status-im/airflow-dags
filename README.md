# Description

This repo contains implementations of Airflow workflows and tasks called respectively [DAGs](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#dags) and [Operators](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#operators).

* DAGs - Direct Acyclic Graphs - Python scripts defining workflows in a way that reflects their relationships.
* Operators - Python functions which define the individual tasks that are executed as part of a DAG run.

To learn how to write DAGs and Operators read about [core concepts](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#concepts) and follow the [official tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html).

# DAG

This repository contains: 

* DAG to launch the Airbyte jobs for the status-website charts, in `website-sync`,
* DAG to launch the Airbyte jobs for the github sync of the different repositories of the org.
* DAG to run all the dbt model in `dbt`,
* DAG to sync data from spiff workflow.


## Development

Each new models can be test on the `test` environment of [infra-bi](https://github.com/status-im/infra-bi) by merging it into the `test` branch of this repo. Once the tests are conclusive, the `test` branch can me merge into the `prod` one.


# Examples

Simple working DAGs taken from Airflow documentation:

* [`bash_commands.py`](examples/bash_commands.py) - Use of `BashOperator` and simple layout.
* [`task_decorator.py`](examples/task_decorator.py) - More complex layout with `DummyOperator`.
* [`task_generator.py`](examples/task_generator.py) - Semi-dynamic way to generate tasks.
* [`taskflow_api.py`](examples/taskflow_api.py) - `SimpleHttpOperator` and `EmailOperator`.

These were researched in [infra-bi#1](https://github.com/status-im/infra-bi/issues/1). More examples are always welcome.

# Continuous Integration

Changes pushed to `master` are automatically fetched to our Airflow instance by the [`airflow-webhook`](https://github.com/status-im/infra-bi/tree/master/ansible/roles/airflow-webhook) service.

# Infrastructure

All Airflow infrastructure is managed in the [infra-bi](https://github.com/status-im/infra-bi) repository.
