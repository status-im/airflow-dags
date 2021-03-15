# Description

This repo contains implementations of Airflow workflows and tasks called respectively [DAGs](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#dags) and [Operators](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#operators).

* DAGs - Direct Acyclic Graphs - Python scripts defining workflows in a way that reflects their relationships.
* Operators - Python functions which define the individual tasks that are executed as part of a DAG run.

To learn how to write DAGs and Operators read about [core concepts](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html#concepts) and follow the [official tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html).

# Examples

Simple working DAGs taken from Airflow documentation:

* [`bash_commands.py`](examples/bash_commands.py) - Use of `BashOperator` and simple layout.
* [`task_decorator.py`](examples/task_decorator.py) - More complex layout with `DummyOperator`.
* [`taskflow_api.py`](examples/taskflow_api.py) - `SimpleHttpOperator` and `EmailOperator`.

These were researched in [infra-bi#1](https://github.com/status-im/infra-bi/issues/1). More examples are always welcome.

# Continuous Integration

Changes pushed to `master` are automatically fetched to our Airflow instance by the [`airflow-webhook`](https://github.com/status-im/infra-bi/tree/master/ansible/roles/airflow-webhook) service.

# Infrastructure

All Airflow infrastructure is managed in the [infra-bi](https://github.com/status-im/infra-bi) repository.
