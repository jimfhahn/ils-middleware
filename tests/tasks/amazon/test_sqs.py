"""Test AWS SQS Operators and functions."""

import pytest
import requests  # type: ignore

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance

from ils_middleware.tasks.amazon.sqs import (
    SubscribeOperator,
    parse_messages,
    get_resource,
)


@pytest.fixture
def test_dag():
    start_date = datetime(2021, 9, 16)
    return DAG("test_dag", default_args={"owner": "airflow", "start_date": start_date})


@pytest.fixture
def mock_variable(monkeypatch):
    def mock_get(key, default=None):
        if key == "SQS_STAGE":
            return "http://aws.com/12345/"

    monkeypatch.setattr(Variable, "get", mock_get)


def test_subscribe_operator_missing_kwargs(test_dag, mock_variable):
    """Test missing kwargs for SubscribeOperator."""

    task = SubscribeOperator(dag=test_dag)
    assert task is not None
    assert task.sqs_queue == "None"
    assert task.aws_conn_id == "aws_sqs_dev"


def test_subscribe_operator(test_dag, mock_variable):
    """Test with typical kwargs for SubscribeOperator."""
    task = SubscribeOperator(queue="stanford-ils", sinopia_env="stage", dag=test_dag)
    assert task.sqs_queue.startswith("http://aws.com/12345/stanford-ils")
    assert task.aws_conn_id == "aws_sqs_stage"


@pytest.fixture
def mock_resource():
    return {
        "user": "jpnelson",
        "group": "stanford",
        "editGroups": ["other", "pcc"],
        "templateId": "ld4p:RT:bf2:Monograph:Instance:Un-nested",
        "types": ["http://id.loc.gov/ontologies/bibframe/Instance"],
        "bfAdminMetadataRefs": [
            "https://api.development.sinopia.io/resource/7f775ec2-4fe8-48a6-9cb4-5b218f9960f1",
            "https://api.development.sinopia.io/resource/bc9e9939-45b3-4122-9b6d-d800c130c576",
        ],
        "bfItemRefs": [],
        "bfInstanceRefs": [],
        "bfWorkRefs": [
            "https://api.development.sinopia.io/resource/6497a461-42dc-42bf-b433-5e47c73f7e89"
        ],
        "id": "7b55e6f7-f91e-4c7a-bbcd-c074485ad18d",
        "uri": "https://api.development.sinopia.io/resource/7b55e6f7-f91e-4c7a-bbcd-c074485ad18d",
        "timestamp": "2021-10-29T20:30:58.821Z",
    }


@pytest.fixture
def mock_message():
    return [
        [
            {
                "Body": """{ "user": { "email": "dscully@stanford.edu"},
                            "resource": { "uri": "https://sinopia.io/1245" }}"""
            }
        ]
    ]


@pytest.fixture
def mock_task_instance(monkeypatch, mock_message, mock_resource):
    def mock_xcom_pull(*args, **kwargs):
        key = kwargs.get("key")
        if key == "resource":
            return mock_resource
        else:
            return mock_message

    def mock_xcom_push(*args, **kwargs):
        return None

    monkeypatch.setattr(TaskInstance, "xcom_pull", mock_xcom_pull)
    monkeypatch.setattr(TaskInstance, "xcom_push", mock_xcom_push)


@pytest.fixture
def mock_get_resource(monkeypatch, mock_resource):
    def mock_get(*args, **kwargs):
        response = requests.models.Response()
        response.status_code = 200
        return response

    def mock_json(*args, **kwargs):
        return mock_resource

    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.setattr(requests.models.Response, "json", mock_json)


def test_parse_messages(
    test_dag, mock_task_instance, mock_variable, mock_get_resource, mock_resource
):
    """Test parse_messages function."""
    task = SubscribeOperator(queue="stanford-ils", sinopia_env="stage", dag=test_dag)
    task_instance = TaskInstance(task, datetime(2021, 10, 12))
    result = parse_messages(task_instance=task_instance)
    assert result == "completed_parse"
    assert task_instance.xcom_pull(key="resource") == mock_resource


def test_get_resource(mock_get_resource, mock_resource):
    resource = get_resource("http://sinopia.io/resource/456abc")
    assert resource == mock_resource


@pytest.fixture
def mock_failed_get_resource(monkeypatch):
    def mock_get(*args, **kwargs):
        response = requests.models.Response()
        response.status_code = 404
        return response

    monkeypatch.setattr(requests, "get", mock_get)


def test_failed_get_resource(mock_failed_get_resource):
    uri = "http://sinopia.io/resource/456abc"
    failed_message = get_resource(uri)
    assert failed_message.startswith(f"{uri} returned error 404")
