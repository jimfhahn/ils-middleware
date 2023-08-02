"""Tests alma Post BF Work"""
import pytest
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from unittest import mock
import requests_mock
from pytest_mock import MockerFixture
from airflow.hooks.base_hook import BaseHook
from tasks import (
    test_task_instance,
    test_alma_api_key,
    test_uri_region,
    mock_task_instance,
)
from ils_middleware.tasks.alma.post_bfwork import NewWorktoAlma


task_instance = mock_task_instance
alma_uri = "https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs?from_nz_mms_id=&from_cz_mms_id=&\
            normalization=&validate=false&override_warning=true&check_match=false&import_profile=&apikey=\
            12ab34c56789101112131415161718192021"


def test_NewWorktoAlma_200(mock_s3_hook, mock_task_instance, mock_env_vars):
    with pytest.raises(Exception, match="Unexpected status code: 400"):
        NewWorktoAlma(
            task_instance=test_task_instance(),
            alma_api_key=test_alma_api_key(),
            uri_region=test_uri_region(),
        )


def test_NewWorktoAlma_400(mock_s3_hook, mock_task_instance, mock_env_vars):
    with requests_mock.Mocker() as m:
        m.post(alma_uri, status_code=400)
        m.put(alma_uri, status_code=400)

        # Call the function and expect it to raise an exception
        with pytest.raises(Exception):
            NewWorktoAlma(
                task_instance=test_task_instance(),
                alma_api_key=test_alma_api_key(),
                uri_region=test_uri_region(),
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
    monkeypatch.setenv(
        "AIRFLOW_VAR_ALMA_API_KEY_PENN", "12ab34c56789101112131415161718192021"
    )
    monkeypatch.setenv(
        "AIRFLOW_VAR_ALMA_URI_REGION_NA", "https://api-na.hosted.exlibrisgroup.com"
    )


@pytest.fixture
def mock_hook(mocker: mock.Mock) -> mock.Mock:
    return mocker.patch("airflow.hooks.base_hook.BaseHook")


mock_s3_hook_with_file_and_key = pytest.mark.usefixtures(
    "mock_env_vars", "mock_s3_hook_with_file_and_key"
)


@pytest.fixture
def mock_s3_hook(monkeypatch):
    def mock_download_file(*args, **kwargs):
        return "tests/fixtures/marc/airflow/4444-5555-6666-7777/alma.xml"

    monkeypatch.setattr(S3Hook, "download_file", mock_download_file)


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
