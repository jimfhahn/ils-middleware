"""Test the AWS S3 tasks properly name and load files."""
import json
import pytest
from datetime import datetime
from unittest import mock

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.taskinstance import TaskInstance

from ils_middleware.tasks.amazon.s3 import get_from_s3, send_to_s3


def test_task():
    return DummyOperator(
        task_id="test_task",
        dag=DAG(
            "test_dag",
            default_args={"owner": "airflow", "start_date": datetime(2021, 9, 20)},
        ),
    )


task_instance = TaskInstance(test_task())
mock_push_store = {}


@pytest.fixture
def mock_env_vars(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_MARC_S3_BUCKET", "sinopia-marc-test")


@pytest.fixture
def mock_s3_hook(monkeypatch):
    def mock_download_file(*args, **kwargs):
        return "tests/fixtures/record.mar"

    monkeypatch.setattr(S3Hook, "download_file", mock_download_file)


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
def mock_s3_load_string():
    with mock.patch(
        "airflow.providers.amazon.aws.hooks.s3.S3Hook.load_string"
    ) as mocked:
        yield mocked


@pytest.fixture
def mock_marc_as_json():
    with open('tests/fixtures/record.json') as data:
        return json.load(data)


def test_get_from_s3(mock_s3_hook, mock_task_instance):
    """Test downloading a file from S3 into a temp file"""
    get_from_s3(task_instance=task_instance)
    assert task_instance.xcom_pull(key="http://example.com/rdf/0000-1111-2222-3333") == "tests/fixtures/record.mar"
    assert task_instance.xcom_pull(key="http://example.com/rdf/4444-5555-6666-7777") == "tests/fixtures/record.mar"


def test_send_to_s3(mock_s3_load_string, mock_task_instance, mock_marc_as_json):
    """Test sending a file to s3"""

    send_to_s3(task_instance=task_instance)
    mock_s3_load_string.call_count == 2
    assert json.loads(task_instance.xcom_pull(key="http://example.com/rdf/0000-1111-2222-3333")) == mock_marc_as_json
    assert json.loads(task_instance.xcom_pull(key="http://example.com/rdf/4444-5555-6666-7777")) == mock_marc_as_json
