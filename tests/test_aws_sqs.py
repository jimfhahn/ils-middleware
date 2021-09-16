"""Test AWS SQS Operators and functions."""

import pytest

from datetime import datetime

from airflow import DAG
from airflow.models import Variable

from dags.aws_sqs import SubscribeOperator


@pytest.fixture
def test_dag():
    start_date = datetime(2021, 9, 16)
    return DAG("test_dag", default_args={"owner": "airflow", "start_date": start_date})


@pytest.fixture
def mock_variable(monkeypatch):
    def mock_get(key):
        if key == "sqs_stage":
            return "http://aws.com/12345/"

    monkeypatch.setattr(Variable, "get", mock_get)


def test_subscribe_operator_missing_kwargs(test_dag, mock_variable):
    """Test missing kwargs for SubscribeOperator."""

    task = SubscribeOperator(dag=test_dag)
    assert task is not None
    assert task.sqs_queue == "None"
    assert task.aws_conn_id == "aws_sqs_dev"


def test_subscribe_operator(test_dag, mock_variable):
    """Test with typical kwargs for SubscribeOperator."""
    task = SubscribeOperator(queue="stanford-ils", sinopia_env="stage", dag=test_dag)
    assert task.sqs_queue.startswith("http://aws.com/12345/stanford-ils")
    assert task.aws_conn_id == "aws_sqs_stage"
