"""Tests Symphony Request"""
import pytest
import requests  # type: ignore

from airflow.hooks.base_hook import BaseHook
from pytest_mock import MockerFixture

from ils_middleware.tasks.symphony.request import SymphonyRequest


@pytest.fixture
def mock_connection(monkeypatch, mocker: MockerFixture):
    def mock_get_connection(*args, **kwargs):
        connection = mocker.stub(name="Connection")
        connection.host = "https://symphony.test.edu/"

        return connection

    monkeypatch.setattr(BaseHook, "get_connection", mock_get_connection)


@pytest.fixture
def mock_request(monkeypatch, mocker: MockerFixture):
    def mock_post(*args, **kwargs):
        new_result = mocker.stub(name="post_result")
        new_result.status_code = 200
        new_result.text = "Successful request"
        new_result.json = lambda: {}
        return new_result

    monkeypatch.setattr(requests, "post", mock_post)


def test_missing_request_filter(mock_connection, mock_request):
    result = SymphonyRequest(token="34567")
    assert result.startswith("Successful request")
