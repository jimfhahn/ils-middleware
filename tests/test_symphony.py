"""Test Symphony Operators and functions."""

import pytest

from datetime import datetime

from airflow import DAG

from dags.symphony import SymphonyLogin


@pytest.fixture
def test_dag():
    start_date = datetime(2021, 9, 20)
    return DAG("test_dag", default_args={"owner": "airflow", "start_date": start_date})


def test_subscribe_operator_missing_kwargs(test_dag):
    """Test missing kwargs for SubscribeOperator."""

    task = SymphonyLogin(dag=test_dag)
    assert task.http_conn_id is None


def test_subscribe_operator(test_dag):
    """Test with typical kwargs for SubscribeOperator."""
    task = SymphonyLogin(
        conn_id="symphony_dev_login", login="DEVSYS", password="APASSWord", dag=test_dag
    )
    assert task.http_conn_id.startswith("symphony_dev_login")
    assert "DEVSYS" in task.data
