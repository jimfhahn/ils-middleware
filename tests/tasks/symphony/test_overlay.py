import pytest
import requests  # type: ignore
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models.taskinstance import TaskInstance

from airflow.hooks.base_hook import BaseHook
from pytest_mock import MockerFixture

from ils_middleware.tasks.symphony.overlay import overlay_marc_in_symphony

CATKEY = "320011"
MARC_JSON = {"leader": "11222999   adf", "fields": [{"tag": "245"}], "catkey": CATKEY}
MARC_JSON_NO_CAT_KEY = {"leader": "11222999   adf", "fields": [{"tag": "245"}]}

def test_task():
    return DummyOperator(
        task_id="test_task",
        dag=DAG(
            "test_dag",
            default_args={"owner": "airflow", "start_date": datetime(2021, 9, 20)},
        ),
    )


@pytest.fixture
def mock_marc_as_json():
    with open('tests/fixtures/record.json') as data:
        return data.read()


task_instance = TaskInstance(test_task())
mock_push_store = {
    "overlay_resources": [
        {
            "resource_uri": "http://example.com/rdf/0000-1111-2222-3333",
            "data": MARC_JSON
        },
        {
            "resource_uri": "http://example.com/rdf/4444-5555-6666-7777",
            "data": MARC_JSON_NO_CAT_KEY
        }
    ]
}


@pytest.fixture
def mock_connection(monkeypatch, mocker: MockerFixture):
    def mock_get_connection(*args, **kwargs):
        connection = mocker.stub(name="Connection")
        connection.host = "https://symphony.test.edu/"

        return connection

    monkeypatch.setattr(BaseHook, "get_connection", mock_get_connection)


@pytest.fixture
def mock_new_request(monkeypatch, mocker: MockerFixture):
    def mock_put(*args, **kwargs):
        new_result = mocker.stub(name="put_result")
        new_result.token = "234566"
        new_result.status_code = 201
        new_result.text = "Successful modified"
        new_result.json = lambda: {"@key": CATKEY}
        return new_result

    monkeypatch.setattr(requests, "put", mock_put)


@pytest.fixture
def mock_task_instance(mock_marc_as_json, monkeypatch): # , mock_resources):
    def mock_xcom_pull(*args, **kwargs):
        key = kwargs.get("key")
        task_ids = kwargs.get("task_ids")
        if task_ids == ["process_symphony.convert_to_symphony_json"]:
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


def test_overlay_marc_in_symphony(mock_new_request, mock_connection, mock_task_instance):
    overlay_marc_in_symphony(
        task_instance=task_instance,
        conn_id="symphony_dev_login",
        session_token="abcde4590",
        catkey=CATKEY,
        marc_json=MARC_JSON,
    )
    assert task_instance.xcom_pull(key='http://example.com/rdf/0000-1111-2222-3333').startswith(CATKEY)


def test_missing_catkey(mock_new_request, mock_connection, mock_task_instance):
    overlay_marc_in_symphony(
        task_instance=task_instance,
        conn_id="symphony_dev_login",
        session_token="abcde4590",
    )
    assert len(task_instance.xcom_pull(key="missing_catkeys")) > 0
