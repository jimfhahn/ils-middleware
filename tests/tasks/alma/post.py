import pytest
from airflow.hooks.base_hook import BaseHook
from pytest_mock import MockerFixture


@pytest.fixture
def mock_connection(monkeypatch, mocker: MockerFixture):
    def mock_get_connection(*args, **kwargs):
        connection = mocker.stub(name="Connection")
        connection.host = "https://alma.test.edu/"

        return connection

    monkeypatch.setattr(BaseHook, "get_connection", mock_get_connection)


@pytest.fixture
def mock_post_request(mocker: MockerFixture):
    def mock_post(*args, **kwargs):
        request = mocker.stub(name="Request")
        request.status_code = 200
        request.text = "<mms_id>123456789</mms_id>"

        return request

    mocker.patch("requests.post", side_effect=mock_post)


pytestmark = pytest.mark.usefixtures("mock_connection", "mock_post_request")
