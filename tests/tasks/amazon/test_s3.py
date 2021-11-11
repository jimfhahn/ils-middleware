"""Test the AWS S3 tasks properly name and load files."""
import json
import pytest
from datetime import datetime
from unittest import mock

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from ils_middleware.tasks.amazon.s3 import get_from_s3, send_to_s3


def test_task():
    return DummyOperator(
        task_id="test_task",
        dag=DAG(
            "test_dag",
            default_args={"owner": "airflow", "start_date": datetime(2021, 9, 20)},
        ),
    )


@pytest.fixture
def mock_env_vars(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_MARC_S3_BUCKET", "sinopia-marc-test")


@pytest.fixture
def mock_s3_hook(monkeypatch):
    def mock_download_file(*args, **kwargs):
        return "path/to/temp/file"

    monkeypatch.setattr(S3Hook, "download_file", mock_download_file)


@pytest.fixture
def mock_s3_load_string():
    with mock.patch(
        "airflow.providers.amazon.aws.hooks.s3.S3Hook.load_string"
    ) as mocked:
        yield mocked


def test_get_from_s3(mock_env_vars, mock_s3_hook):
    """Test downloading a file from S3 into a temp file"""
    result = get_from_s3(instance_id="0000-1111-2222-3333")
    assert json.loads(result) == {
        "id": "0000-1111-2222-3333",
        "temp_file": "path/to/temp/file",
    }


def test_send_to_s3(mock_env_vars, mock_s3_load_string):
    """Test sending a file to s3"""

    send_to_s3(
        instance="""{"id": "0000-1111-2222-3333", "temp_file": "tests/fixtures/record.mar"}"""
    )
    mock_s3_load_string.assert_called_once()
