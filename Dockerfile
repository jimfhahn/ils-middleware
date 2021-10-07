FROM apache/airflow:2.1.2-python3.8

ENV POETRY_VERSION=1.1.8

USER root
RUN apt-get -y update && apt-get -y install git
USER airflow

RUN pip install "poetry==$POETRY_VERSION"
COPY --chown=airflow:root poetry.lock pyproject.toml /opt/airflow/
COPY --chown=airflow:root ./ils_middleware /opt/airflow/ils_middleware

COPY --chown=airflow:root dist/ils_middleware-0.1.0-py3-none-any.whl .
RUN pip install ils_middleware-0.1.0-py3-none-any.whl

RUN poetry config virtualenvs.create false \ 
    && poetry install --no-dev --no-interaction --no-ansi

