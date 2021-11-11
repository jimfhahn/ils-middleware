"""Test Symphony Operators and functions."""
import io

import pytest
from datetime import datetime
from urllib.parse import urlparse
from os import path

from airflow import DAG

from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook
from airflow.models.taskinstance import TaskInstance
from airflow.operators.dummy import DummyOperator

from ils_middleware.tasks.sinopia.rdf2marc import Rdf2Marc

mock_200_response = {
    "Payload": io.StringIO("{}"),
    "ResponseMetadata": {"HTTPHeaders": {}},
    "StatusCode": 200,
}

mock_push_store = {}

def test_task():
    start_date = datetime(2021, 9, 20)
    test_dag = DAG(
        "test_dag", default_args={"owner": "airflow", "start_date": start_date}
    )
    return DummyOperator(task_id="test", dag=test_dag)


task_instance = TaskInstance(test_task())


@pytest.fixture
def mock_task_instance(monkeypatch): # , mock_resources):
    def mock_xcom_pull(*args, **kwargs):
        key = kwargs.get("key")
        if key == "resources":
            return ["http://example.com/rdf/0000-1111-2222-3333", "http://example.com/rdf/4444-5555-6666-7777"]
        else:
            return mock_push_store[key]

    def mock_xcom_push(*args, **kwargs):
        key = kwargs.get("key")
        value = kwargs.get("value")
        mock_push_store[key] = value
        return None

    monkeypatch.setattr(TaskInstance, "xcom_pull", mock_xcom_pull)
    monkeypatch.setattr(TaskInstance, "xcom_push", mock_xcom_push)


@pytest.fixture
def mock_lambda(monkeypatch):
    def mock_invoke_lambda(*args, **kwargs):
        return mock_200_response

    monkeypatch.setattr(AwsLambdaHook, "invoke_lambda", mock_invoke_lambda)


def test_Rdf2Marc(mock_task_instance, mock_lambda):
    Rdf2Marc(task_instance=task_instance)
    assert task_instance.xcom_pull(key="http://example.com/rdf/0000-1111-2222-3333") == "airflow/0000-1111-2222-3333/record.mar"
    assert task_instance.xcom_pull(key="http://example.com/rdf/4444-5555-6666-7777") == "airflow/4444-5555-6666-7777/record.mar"


@pytest.fixture
def mock_failed_lambda(monkeypatch):
    def mock_invoke_lambda(*args, **kwargs):
        return {
            "StatusCode": 200,
            "Payload": io.StringIO(
                """{ "errorMessage": "AdminMetadata (bf:adminMetadata) not specified for Instance"}"""
            ),
            "ResponseMetadata": {"HTTPHeaders": {"x-amz-function-error": "Unhandled"}},
        }

    monkeypatch.setattr(AwsLambdaHook, "invoke_lambda", mock_invoke_lambda)


def test_Rdf2Marc_LambdaError(mock_task_instance, mock_failed_lambda):
    Rdf2Marc(task_instance=task_instance)
    assert task_instance.xcom_pull(key="http://example.com/rdf/0000-1111-2222-3333") == {"error_message": "RDF2MARC conversion failed for http://example.com/rdf/0000-1111-2222-3333, error: AdminMetadata (bf:adminMetadata) not specified for Instance"}
    assert task_instance.xcom_pull(key="http://example.com/rdf/4444-5555-6666-7777") == {"error_message": "RDF2MARC conversion failed for http://example.com/rdf/4444-5555-6666-7777, error: AdminMetadata (bf:adminMetadata) not specified for Instance"}
