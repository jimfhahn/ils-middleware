import pytest
import json
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models.taskinstance import TaskInstance


def test_task():
    return DummyOperator(
        task_id="test_task",
        dag=DAG(
            "test_dag",
            default_args={"owner": "airflow", "start_date": datetime(2021, 9, 20)},
        ),
    )


def test_task_instance():
    return TaskInstance(test_task())


mock_push_store = {}


@pytest.fixture
def mock_task_instance(monkeypatch):
    def mock_xcom_pull(*args, **kwargs):
        key = kwargs.get("key")
        if key == "resources":
            return [
                "http://example.com/rdf/0000-1111-2222-3333",
                "http://example.com/rdf/4444-5555-6666-7777",
            ]
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
def mock_marc_as_json():
    with open("tests/fixtures/record.json") as data:
        return json.load(data)
