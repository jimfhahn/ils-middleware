"""Tests new MARC record in Symphony task."""

from datetime import datetime
import pytest

from airflow import DAG
from dags.tasks.symphony.new import NewMARCtoSymphony


@pytest.fixture
def test_dag():
    start_date = datetime(2021, 9, 20)
    return DAG("test_dag", default_args={"owner": "airflow", "start_date": start_date})


def test_NewMARCtoSymphony(test_dag):
    """Tests NewMARCtoSymphony"""
    task = NewMARCtoSymphony(
        conn_id="symphony_dev_login",
        session_token="abcdefag",
        library_key="GREEN",
        marc_json={"leader": "basdfdaf   adf", "fields": [{"tag": "245"}]},
        dag=test_dag,
    )
    assert task.http_conn_id.startswith("symphony_dev_login")
    assert task.endpoint.startswith("catalog/bib")
    assert "basdfdaf" in task.data
