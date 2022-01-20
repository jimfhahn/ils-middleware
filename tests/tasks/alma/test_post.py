"""Tests alma Post"""
import pytest
import lxml.etree as ET
import requests

# import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# from airflow.models import Variable
# from airflow import models
from unittest import mock
from airflow.hooks.base_hook import BaseHook
from pytest_mock import MockerFixture
from tasks import (
    test_task_instance,
    test_alma_api_key,
    test_import_profile_id,
    mock_task_instance,
    test_xml_response,
)
from ils_middleware.tasks.alma.post import NewMARCtoAlma

task_instance = mock_task_instance
xml_response = test_xml_response
doc = xml_response


def test_NewMARCtoAlma(mock_s3_hook, mock_task_instance, mock_env_vars):
    NewMARCtoAlma(
        task_instance=test_task_instance(),
        alma_api_key=test_alma_api_key(),
        alma_import_profile_id=test_import_profile_id(),
    )


@pytest.fixture()
def mock_etree():
    with mock.patch("organizer.tools.xml_files_operations.etree") as mocked_etree:
        yield mocked_etree


@pytest.fixture()
def mock_parser(mock_etree):
    parser = mock.Mock()
    with mock.patch.object(mock_etree, "XMLParser", parser):
        yield parser


"fixture for .find and .text"


@pytest.fixture
def mock_find(mock_etree):
    def mock_find(self, *args, **kwargs):
        return ET.Element(
            "root",
            attrib={
                "mms_id": "mms_id",
                "status": "status",
                "error_message": "error_message",
                "error_code": "error_code",
            },
        )

    with mock.patch.object(mock_etree, "Element", mock_find):
        yield mock_find


@pytest.fixture
def mock_text(mock_etree):
    def mock_text(self, *args, **kwargs):
        return "text"

    with mock.patch.object(mock_etree, "Text", mock_text):
        yield mock_text


@pytest.fixture
def mock_request(monkeypatch, mocker: MockerFixture):
    def mock_post(*args, **kwargs):
        mms_id = test_xml_response.find("mms_id").text = "9978021305103681"
        return mms_id

    monkeypatch.setattr(requests, "post", mock_post)


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
        "AIRFLOW_VAR_ALMA_SANDBOX_API_KEY", "12ab34c56789101112131415161718192021"
    )
    monkeypatch.setenv("AIRFLOW_VAR_IMPORT_PROFILE_ID", "33008879050003681")


@pytest.fixture
def mock_hook(mocker: mock.Mock) -> mock.Mock:
    return mocker.patch("airflow.hooks.base_hook.BaseHook")


mock_s3_hook_with_file_and_key = pytest.mark.usefixtures(
    "mock_env_vars", "mock_s3_hook_with_file_and_key"
)  # noqa: E501
"""Test the Alma AWS S3 tasks properly name and load files."""


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
