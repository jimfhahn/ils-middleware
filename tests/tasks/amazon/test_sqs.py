"""Test AWS SQS Operators and functions."""

import pytest

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance

from ils_middleware.tasks.amazon.sqs import SubscribeOperator, parse_messages


@pytest.fixture
def test_dag():
    start_date = datetime(2021, 9, 16)
    return DAG("test_dag", default_args={"owner": "airflow", "start_date": start_date})


@pytest.fixture
def mock_variable(monkeypatch):
    def mock_get(key, default=None):
        if key == "SQS_STAGE":
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


@pytest.fixture
def mock_task_instance(monkeypatch):
    def mock_xcom_pull(*args, **kwargs):
        return [
            [
                {
                    "Body": """{ "user": { "email": "dscully@stanford.edu"},
                                "resource": { "uri": "https://sinopia.io/1245" }}"""
                }
            ]
        ]

    def mock_xcom_push(*args, **kwargs):
        return None

    monkeypatch.setattr(TaskInstance, "xcom_pull", mock_xcom_pull)
    monkeypatch.setattr(TaskInstance, "xcom_push", mock_xcom_push)


def test_parse_messages(test_dag, mock_task_instance, mock_variable):
    """Test parse_messages function."""
    task = SubscribeOperator(queue="stanford-ils", sinopia_env="stage", dag=test_dag)
    task_instance = TaskInstance(task, datetime(2021, 10, 12))
    result = parse_messages(task_instance=task_instance)
    assert result == "completed_parse"
