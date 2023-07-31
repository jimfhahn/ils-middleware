"""Tests alma Post BF Work"""
import pytest
import lxml.etree as ET
import requests


from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from unittest import mock
from airflow.hooks.base_hook import BaseHook
from pytest_mock import MockerFixture
from tasks import (
    test_task_instance,
    test_alma_api_key,
    test_uri_region,
    mock_task_instance,
    test_xml_response,
)
from ils_middleware.tasks.alma.post_bfwork import NewWorktoAlma

task_instance = mock_task_instance
xml_response = test_xml_response
doc = xml_response


def test_WorktoAlma(mock_s3_hook, mock_task_instance, mock_env_vars):
    NewWorktoAlma(
        task_instance=test_task_instance(),
        alma_api_key=test_alma_api_key(),
        uri_region=test_uri_region(),
    )


@pytest.fixture()
def mock_parser(mock_etree):
    parser = mock.Mock()
    with mock.patch.object(mock_etree, "XMLParser", parser):
        yield parser


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
def mock_request(monkeypatch):
    class MockResponse(object):
        def __init__(self, status_code, text="", content=None):
            self.status_code = status_code
            self.text = text
            self.content = content

        def xml_response_data(self):
            return ET.fromstring(self.text)

        def mock_post(*args, **kwargs):
            if args[0] == 400:
                return MockResponse(400, text="Bad Request", content=b"Bad Request")
            return MockResponse(200, text="OK", content=b"OK")

        def mock_put(*args, **kwargs):
            if args[0] == 400:
                return MockResponse(400, text="Bad Request", content=b"Bad Request")
            return MockResponse(200, text="OK", content=b"OK")

    mock_response = MockResponse(200, text="OK", content=b"OK")
    monkeypatch.setattr(requests, "post", mock_response.mock_post)
    monkeypatch.setattr(requests, "put", mock_response.mock_put)

    return mock_response


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


def test_mms_id_extraction(mocker):
    instance_uri = "https://api.sinopia.io/resource/12345"
    mock_s3_hook = mocker.patch("airflow.providers.amazon.aws.hooks.s3.S3Hook")
    mock_s3_hook.download_file.return_value = "temp_file_path"
    mock_request = mocker.patch("requests.post")
    mock_request.return_value.status_code = 200
    mock_request.return_value.content = b"<mms_id>12345</mms_id>"
    mock_task_instance = mocker.Mock()
    mock_task_instance.xcom_pull.return_value = "temp_file_path"
    mock_etree = mocker.patch("lxml.etree.ElementTree")
    mock_xml_response = mock_etree.ElementTree()
    mock_xml_response.xpath.return_value = ["12345"]

    new_work_to_alma = NewWorktoAlma(
        task_instance=mock_task_instance,
        resources=[instance_uri],
        alma_api_key=test_alma_api_key(),
        uri_region=test_uri_region(),
    )

    mock_xml_response.xpath.assert_called_once_with("//mms_id/text()")
    mock_task_instance.xcom_push.assert_called_once_with(
        key=instance_uri, value="12345"
    )
    assert new_work_to_alma.task_instance == mock_task_instance


def test_work_to_alma_400_status_code(
    mock_s3_hook, mock_request, mock_task_instance, mock_etree
):
    instance_uri = "https://api.sinopia.io/resource/12345"
    mock_s3_hook.download_file.return_value = "temp_file_path"
    mock_request.mock_post.return_value.status_code = 400  # Simulate a 400 status code
    mock_request.mock_post.return_value.content = b"<mms_id>12345</mms_id>"
    mock_task_instance.xcom_pull.return_value = "temp_file_path"
    mock_xml_response = mock_etree.ElementTree()
    mock_xml_response.xpath.return_value = ["12345"]

    with pytest.raises(Exception, match="Unexpected status code: 400"):
        NewWorktoAlma(
            task_instance=mock_task_instance,
            resources=[instance_uri],
            alma_api_key=test_alma_api_key(),
            uri_region=test_uri_region(),
        )
