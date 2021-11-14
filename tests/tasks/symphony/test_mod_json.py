"""Tests functions for modifying JSON to Symphony JSON."""
import pytest
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models.taskinstance import TaskInstance

from ils_middleware.tasks.symphony.mod_json import to_symphony_json


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
        if key == "resources":
            return [
                "http://example.com/rdf/0000-1111-2222-3333",
                "http://example.com/rdf/4444-5555-6666-7777",
            ]
        elif task_ids == ["process_symphony.marc_json_to_s3"]:
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


def test_to_symphony_json(mock_task_instance):
    to_symphony_json(task_instance=task_instance)
    symphony_json = task_instance.xcom_pull(
        key="http://example.com/rdf/0000-1111-2222-3333"
    )
    assert symphony_json["standard"].startswith("MARC21")
    assert symphony_json["leader"].startswith("01455nam a2200277uu 4500")
    assert symphony_json["fields"][0]["tag"] == "003"
    assert symphony_json["fields"][2]["subfields"][0]["code"] == "a"
    assert symphony_json["fields"][2]["subfields"][0]["data"] == "981811"
    assert symphony_json["fields"][7]["subfields"][0]["data"].startswith(
        "Hildegard von Bingen's Physica"
    )
