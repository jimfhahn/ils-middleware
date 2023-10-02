"""Tests alma Post BF Work"""
import pytest
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from unittest import mock
from unittest.mock import Mock
from pytest_mock import MockerFixture
from airflow.hooks.base_hook import BaseHook
from unittest.mock import patch


from tasks import (
    test_task_instance,
    test_alma_api_key,
    test_uri_region,
    mock_task_instance,
)
from ils_middleware.tasks.alma.post_bfwork import (
    NewWorktoAlma,
    parse_400,
    putWorkToAlma,
    get_env_vars,
)

task_instance = mock_task_instance
alma_uri = "https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs?from_nz_mms_id=&from_cz_mms_id=&\
            normalization=&validate=false&override_warning=true&check_match=false&import_profile=&apikey=\
            12ab34c56789101112131415161718192021"
MockDag = Mock()
MockDag.dag_id = "penn"
actual_dag = MockDag


def test_get_env_vars():
    with patch("airflow.models.Variable.get") as mock_get:
        mock_get.return_value = "dummy_value"

        uri_region, alma_api_key = get_env_vars("penn")

        assert uri_region == "dummy_value"
        assert alma_api_key == "dummy_value"


@pytest.fixture
def uri_region():
    return "dummy_value"


@pytest.fixture
def alma_api_key():
    return "dummy_value"


@pytest.fixture
def data():
    return "dummy_value"


@pytest.fixture
def instance_uri():
    return "dummy_value"


def test_NewWorktoAlma_400(
    mock_s3_hook,
    mock_task_instance,
    mock_env_vars,
    uri_region,
    alma_api_key,
    data,
    instance_uri,
):
    # Mock the requests module and return a mock response with a 400 status code and a valid XML response body
    mock_response = Mock()
    mock_response.status_code = 400
    mock_response.content = (
        b"<root><error><errorMessage1>Test Error</errorMessage1></error></root>"
    )
    with patch("requests.post", return_value=mock_response):
        # Call the function with mock arguments
        task_instance = Mock()
        # call xcom_push
        task_instance.xcom_pull.return_value = [
            "https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs/12345"
        ]
        with pytest.raises(Exception) as e:
            NewWorktoAlma(
                task_instance=test_task_instance(),
                alma_api_key=test_alma_api_key(),
                uri_region=test_uri_region(),
                dag=MockDag,
            )
        assert "Internal server error from Alma API: 500" in str(e.value)


def test_NewWorktoAlma_200(mock_s3_hook, mock_task_instance, mock_env_vars):
    # Mock the requests module and return a mock response with a 200 status code and a valid XML response body
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = b"<root><mms_id>12345</mms_id></root>"
    with patch("requests.post", return_value=mock_response):
        # Call the function with mock arguments
        task_instance = Mock()
        # call xcom_push
        task_instance.xcom_pull.return_value = [
            "https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs/12345"
        ]
        NewWorktoAlma(
            task_instance=test_task_instance(),
            alma_api_key=test_alma_api_key(),
            uri_region=test_uri_region(),
            dag=MockDag,
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
        "AIRFLOW_VAR_ALMA_URI_REGION_PENN", "https://api-na.hosted.exlibrisgroup.com"
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


def test_parse_400():
    # Create a mock result
    result = "<root><error><errorMessage1>Test Error</errorMessage1></error></root>"
    # Call the function
    put_mms_id_str = parse_400(result)
    # Assert that the function returns the expected value
    assert put_mms_id_str == "No text found in brackets"


def test_putWorkToAlma_success():
    # Mock the requests module and return a mock response with a 200 status code
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.content = b"<root><mms_id>12345</mms_id></root>"
    with patch("requests.put", return_value=mock_response):
        # Call the function with mock arguments
        task_instance = Mock()
        putWorkToAlma(
            alma_update_uri="https://example.com",
            data="<root></root>",
            task_instance=task_instance,
            instance_uri="test_instance_uri",
        )
        # Assert that the xcom_push method is called with the expected arguments
        task_instance.xcom_push.assert_called_once_with(
            key="test_instance_uri", value=["12345"]
        )


def test_putWorkToAlma_failure():
    # Mock the requests module and return a mock response with a non-200 status code
    mock_response = Mock()
    mock_response.status_code = 400
    with patch("requests.put", return_value=mock_response):
        # Call the function with mock arguments and expect it to raise an exception
        task_instance = Mock()
        with pytest.raises(Exception):
            putWorkToAlma(
                alma_update_uri="https://example.com",
                data="<root></root>",
                task_instance=task_instance,
                instance_uri="test_instance_uri",
            )
