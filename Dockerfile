FROM apache/airflow:2.10.1-python3.12

ENV POETRY_VERSION=1.8.3

USER root
RUN apt-get -y update && apt-get -y install git gcc g++
USER airflow

RUN pip install "poetry==$POETRY_VERSION"
COPY --chown=airflow:root poetry.lock pyproject.toml /opt/airflow/
COPY --chown=airflow:root ./ils_middleware /opt/airflow/ils_middleware

RUN poetry build --format=wheel
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" dist/*.whl
