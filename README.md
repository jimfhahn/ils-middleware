# ILS Middleware
Proof-of-concept for using [Apache Airflow][AF] to manage Sinopia workflows
that interact with institutional integrated library systems (ILS) and/or
library services platform (LSP). Currently there are Directed Acyclic Graphs (DAG)
for Stanford and Cornell Sinopia-to-ILS/LSP workflows.

## Running Locally with Docker
Based on the documentation, [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html):

1. Clone repository `git clone https://github.com/LD4P/ils-middleware`
1. Run `docker-compose up airflow-init` to initialize the Airflow
1. Bring up airflow, `docker-compose up` to run the containers in the foreground,
   add `docker-compose up -d` to run as a daemon.
1. Access Airflow locally at http://localhost:8080

## Editing existing DAGs
The `dags/stanford.py` contains Stanford's Symphony and FOLIO workflows from
Sinopia editor user initiated process. The `dags/cornell.py` DAG is for Cornell's
FOLIO workflow. Editing either of these code files will change the DAG.

## Adding a new DAG
In the `dags` subdirectory, add a python module for the DAG. Running Airflow
locally through Docker (see above), the DAG should appear in the list of DAGs
or display any errors in the DAG.

## Dependency Management and Packaging
We are using [poetry][POET] to better manage dependency updates. To install
[poetry][POET], run the following command in your shell:

`curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -`

[AF]: https://airflow.apache.org/
[POET]: https://python-poetry.org/
