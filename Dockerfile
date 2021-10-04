FROM apache/airflow:2.1.2-python3.8

USER root
RUN apt-get -y update && apt-get -y install git
USER airflow

COPY --chown=airflow:root dist/ils_middleware-0.1.0-py3-none-any.whl .
RUN pip install ils_middleware-0.1.0-py3-none-any.whl
COPY --chown=airflow:root ./ils_middleware/dags /opt/airflow/dags