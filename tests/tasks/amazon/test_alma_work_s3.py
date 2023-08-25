"""Test the Alma AWS S3 tasks properly name and load files."""
import pytest
from pytest_mock import MockerFixture

from unittest import mock

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from ils_middleware.tasks.amazon.alma_work_s3 import (
    get_from_alma_s3,
    send_work_to_alma_s3,
)

from airflow.hooks.base_hook import BaseHook

from tasks import mock_task_instance, test_task_instance

import ssl

ssl._create_default_https_context = ssl._create_unverified_context

task_instance = mock_task_instance
marc = "tests/fixtures/record.mar"
work = "tests/fixtures/work-rdf-c22129c79459.xml"


def test_get_from_alma_s3(mock_s3_hook, mock_env_vars, mock_task_instance):
    get_from_alma_s3(task_instance=test_task_instance())
    assert test_task_instance().xcom_pull(
        key="https://api.development.sinopia.io/resource/0000-1111-2222-3333"
    )


def test_send_work_to_alma_s3(
    mock_env_vars,
    mock_s3_load_string,
    mock_s3_hook,
    mock_connection,
    mock_task_instance,
):
    """Test sending a file to s3"""
    send_work_to_alma_s3(task_instance=test_task_instance())
    assert test_task_instance().xcom_pull(
        key="https://api.development.sinopia.io/resource/0000-1111-2222-3333"
    )


@pytest.fixture
def mock_connection(monkeypatch, mocker: MockerFixture):
    def mock_get_connection(*args, **kwargs):
        connection = mocker.stub(name="Connection")
        connection.host = "https://alma.test.edu/"

        return connection

    monkeypatch.setattr(BaseHook, "get_connection", mock_get_connection)


@pytest.fixture
def mock_env_vars(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_MARC_S3_BUCKET", "sinopia-marc-test")


@pytest.fixture
def mock_hook(mocker: mock.Mock) -> mock.Mock:
    return mocker.patch("airflow.hooks.base_hook.BaseHook")


mock_s3_hook_with_file_and_key = pytest.mark.usefixtures(
    "mock_env_vars", "mock_s3_hook_with_file_and_key"
)
"""Test the Alma AWS S3 tasks properly name and load files."""


@pytest.fixture
def mock_s3_hook(monkeypatch):
    def mock_download_file(*args, **kwargs):
        return marc

    def mock_load_bytes(*args, **kwargs):
        return

    monkeypatch.setattr(S3Hook, "download_file", mock_download_file)
    monkeypatch.setattr(S3Hook, "load_bytes", mock_load_bytes)


@pytest.fixture
def mock_s3_load_string():
    with mock.patch(
        "airflow.providers.amazon.aws.hooks.s3.S3Hook.load_string"
    ) as mocked:
        yield mocked


@pytest.fixture
def mock_s3_load_bytes():
    with mock.patch(
        "airflow.providers.amazon.aws.hooks.s3.S3Hook.load_bytes"
    ) as mocked:
        yield mocked
