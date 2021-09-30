"""Test Symphony Operators and functions."""
import pytest
from datetime import datetime

from airflow import DAG

from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook
from airflow.models.taskinstance import TaskInstance
from airflow.operators.dummy import DummyOperator

from ils_middleware.tasks.sinopia.rdf2marc import Rdf2Marc

mock_200_response = {
    "StatusCode": 200,
}


def test_task():
    start_date = datetime(2021, 9, 20)
    test_dag = DAG(
        "test_dag", default_args={"owner": "airflow", "start_date": start_date}
    )
    return DummyOperator(task_id="test", dag=test_dag)


@pytest.fixture
def mock_task_instance(monkeypatch):
    def mock_xcom_pull(*args, **kwargs):
        return "http://example.com/rdf/0000-1111-2222-3333"

    monkeypatch.setattr(TaskInstance, "xcom_pull", mock_xcom_pull)


@pytest.fixture
def mock_lambda(monkeypatch):
    def mock_invoke_lambda(*args, **kwargs):
        return mock_200_response

    monkeypatch.setattr(AwsLambdaHook, "invoke_lambda", mock_invoke_lambda)


def test_Rdf2Marc(mock_task_instance, mock_lambda):
    payload = {"instance_uri": "http://example.com/rdf/0000-1111-2222-3333"}
    execution_date = datetime(2021, 9, 21)
    task_instance = TaskInstance(test_task(), execution_date)

    assert (
        Rdf2Marc(task_instance=task_instance, payload=payload) == "0000-1111-2222-3333"
    )
