"""Tests new MARC record in Symphony task."""


import pytest
import requests  # type: ignore
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models.taskinstance import TaskInstance

from airflow.hooks.base_hook import BaseHook
from pytest_mock import MockerFixture

from ils_middleware.tasks.symphony.new import NewMARCtoSymphony


def test_task():
    return DummyOperator(
        task_id="test_task",
        dag=DAG(
            "test_dag",
            default_args={"owner": "airflow", "start_date": datetime(2021, 9, 20)},
        ),
    )


task_instance = TaskInstance(test_task())
mock_push_store = {}


@pytest.fixture
def mock_marc_as_json():
    with open("tests/fixtures/record.json") as data:
        return data.read()


@pytest.fixture
def mock_task_instance(mock_marc_as_json, monkeypatch):
    def mock_xcom_pull(*args, **kwargs):
        key = kwargs.get("key")
        task_ids = kwargs.get("task_ids")
        if key == "new_resources":
            return [
                "http://example.com/rdf/0000-1111-2222-3333",
                "http://example.com/rdf/4444-5555-6666-7777",
            ]
        elif task_ids == ["process_symphony.convert_to_symphony_json"]:
            return mock_marc_as_json
        else:
            return mock_push_store[key]

    def mock_xcom_push(*args, **kwargs):
        key = kwargs.get("key")
        value = kwargs.get("value")
        mock_push_store[key] = value
        return None

    monkeypatch.setattr(TaskInstance, "xcom_pull", mock_xcom_pull)
    monkeypatch.setattr(TaskInstance, "xcom_push", mock_xcom_push)


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


def test_NewMARCtoSymphony(mock_connection, mock_new_request, mock_task_instance):
    """Tests NewMARCtoSymphony"""
    NewMARCtoSymphony(
        task_instance=task_instance,
        conn_id="symphony_dev_login",
        session_token="abcde4590",
        library_key="GREEN",
        marc_json="""{"leader": "11222999   adf", "fields": [{"tag": "245"}]}""",
    )
    assert (
        task_instance.xcom_pull(key="http://example.com/rdf/0000-1111-2222-3333")
        == "45678"
    )


@pytest.fixture
def mock_failed_request(monkeypatch, mocker: MockerFixture):
    def mock_failed_post(*args, **kwargs):
        failed_result = mocker.stub(name="post_result")
        failed_result.status_code = 401
        failed_result.text = "Unauthorized"
        return failed_result

    monkeypatch.setattr(requests, "post", mock_failed_post)


# def test_NewMARCtoSymphony_failed(mock_connection, mock_failed_request, mock_task_instance):
#     """Test failed NewMARCtoSymphony"""
#     with pytest.raises(Exception, match="Symphony Web Service"):
#         NewMARCtoSymphony(
#             task_instance=task_instance,
#             conn_id="symphony_dev_login",
#             session_token="abcde3456",
#             library_key="GREEN",
#             marc_json="""{"leader": "11222999   adf", "fields": [{"tag": "245"}]}""",
#         )
