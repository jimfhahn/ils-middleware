[![CircleCI](https://circleci.com/gh/LD4P/ils-middleware/tree/main.svg?style=svg)](https://circleci.com/gh/LD4P/ils-middleware/tree/main)

# ILS Middleware
Proof-of-concept for using [Apache Airflow][AF] to manage Sinopia workflows
that interact with institutional integrated library systems (ILS) and/or
library services platform (LSP). Currently there are Directed Acyclic Graphs (DAG)
for Stanford and Cornell Sinopia-to-ILS/LSP workflows.

## Running Locally with Docker
Based on the documentation, [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html):

1. Clone repository `git clone https://github.com/LD4P/ils-middleware`
1. If it's commented out, uncomment the line `- ./dags:/opt/airflow/dags` in docker-compose.yaml (under `volumes`, under `x-airflow-common`).
1. Run `docker compose up airflow-init` to initialize the Airflow
1. Bring up airflow, `docker compose up` to run the containers in the foreground,
   add `docker compose up -d` to run as a daemon.
1. Access Airflow locally at http://localhost:8080

## Editing existing DAGs
The `dags/stanford.py` contains Stanford's Symphony and FOLIO workflows from
Sinopia editor user initiated process. The `dags/cornell.py` DAG is for Cornell's
FOLIO workflow. Editing either of these code files will change the DAG.

## Adding a new DAG
In the `dags` subdirectory, add a python module for the DAG. Running Airflow
locally through Docker (see above), the DAG should appear in the list of DAGs
or display any errors in the DAG.

To add any new DAGs to `ld4p/ils-middleware:latest` image, you can either
* build the image locally with `docker build -t ld4p/ils-middleware:latest .` or,
* if commented out, uncomment the `build: .` line (under `x-airflow-common`) in `docker-compose.yaml`
while commenting out the previous line `image: ${AIRFLOW_IMAGE_NAME:-ld4p/ils-middleware:latest}`.

## Dependency Management and Packaging
We are using [poetry][POET] to better manage dependency updates. To install
[poetry][POET], run the following command in your shell:

`curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -`

## Automated Tests

The [pytest][PYTEST] framework is used to run the tests.  Tests can be invoked manually by calling `poetry run pytest` (which will save an xml formatted [coverage report][PYTESTCOV], as well as printing the coverage report to the terminal).

## Linting

The [flake8][FLK8] Python code linter can be manually run by invoking `poetry run flake8` from
the command-line. Configuration options are in the `setup.cfg` file, under the flake8 section.

## Code formatting

Code can be auto-formatted using [Black][BLACK], an opinionated Python code formatter.

To see whether Black would make changes: `poetry run black --check .`

To have Black apply formatting: `poetry run black .`

For information about integrations, including Git hooks and plugins for popular IDEs, see:  https://black.readthedocs.io/en/stable/integrations/index.html

[AF]: https://airflow.apache.org/
[BLACK]: https://black.readthedocs.io/
[FLK8]: https://flake8.pycqa.org/en/latest/
[POET]: https://python-poetry.org/
[PYTEST]: https://docs.pytest.org/
[PYTESTCOV]: https://github.com/pytest-dev/pytest-cov
