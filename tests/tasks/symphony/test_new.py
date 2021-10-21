"""Tests new MARC record in Symphony task."""


import pytest
import requests  # type: ignore

from airflow.hooks.base_hook import BaseHook
from pytest_mock import MockerFixture

from ils_middleware.tasks.symphony.new import NewMARCtoSymphony


@pytest.fixture
def mock_connection(monkeypatch, mocker: MockerFixture):
    def mock_get_connection(*args, **kwargs):
        connection = mocker.stub(name="Connection")
        connection.host = "https://symphony.test.edu/"

        return connection

    monkeypatch.setattr(BaseHook, "get_connection", mock_get_connection)


@pytest.fixture
def mock_new_request(monkeypatch, mocker: MockerFixture):
    def mock_post(*args, **kwargs):
        new_result = mocker.stub(name="post_result")
        new_result.token = "234566"
        new_result.status_code = 200
        new_result.text = "Successful creation"
        new_result.json = lambda: {"@key": "45678"}
        return new_result

    monkeypatch.setattr(requests, "post", mock_post)


def test_NewMARCtoSymphony(mock_connection, mock_new_request):
    """Tests NewMARCtoSymphony"""
    task_result = NewMARCtoSymphony(
        conn_id="symphony_dev_login",
        session_token="abcde4590",
        library_key="GREEN",
        marc_json="""{"leader": "11222999   adf", "fields": [{"tag": "245"}]}""",
    )
    assert "45678" == task_result


@pytest.fixture
def mock_failed_request(monkeypatch, mocker: MockerFixture):
    def mock_failed_post(*args, **kwargs):
        failed_result = mocker.stub(name="post_result")
        failed_result.status_code = 401
        failed_result.text = "Unauthorized"
        return failed_result

    monkeypatch.setattr(requests, "post", mock_failed_post)


def test_NewMARCtoSymphony_failed(mock_connection, mock_failed_request):
    """Test failed NewMARCtoSymphony"""
    with pytest.raises(Exception, match="Symphony Web Service"):
        NewMARCtoSymphony(
            conn_id="symphony_dev_login",
            session_token="abcde3456",
            library_key="GREEN",
            marc_json="""{"leader": "11222999   adf", "fields": [{"tag": "245"}]}""",
        )
