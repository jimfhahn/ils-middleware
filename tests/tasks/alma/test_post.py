"""Tests alma Post"""
import pytest
import requests  # type: ignore
from airflow.hooks.base_hook import BaseHook
from pytest_mock import MockerFixture
from tasks import test_task_instance, mock_task_instance  # noqa: F401


task_instance = test_task_instance()


@pytest.fixture
def mock_failed_request(monkeypatch, mocker: MockerFixture):
    def mock_failed_post(*args, **kwargs):
        failed_result = mocker.stub(name="post_result")
        failed_result.status_code = 401
        failed_result.text = "Unauthorized"
        return failed_result

    monkeypatch.setattr(requests, "post", mock_failed_post)


@pytest.fixture
def mock_sinopia_env(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_SINOPIA_ENV", "test")


@pytest.fixture
def mock_request(monkeypatch, mocker: MockerFixture):
    def mock_post(*args, **kwargs):
        new_result = mocker.stub(name="post_result")
        new_result.status_code = 200
        new_result.text = "Successful request"
        new_result.json = lambda: {}
        return new_result

    monkeypatch.setattr(requests, "post", mock_post)


@pytest.fixture
def mock_connection(monkeypatch, mocker: MockerFixture):
    def mock_get_connection(*args, **kwargs):
        connection = mocker.stub(name="Connection")
        connection.host = "https://alma.test.edu/"

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
