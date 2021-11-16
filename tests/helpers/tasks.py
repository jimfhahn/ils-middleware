import pytest
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models.taskinstance import TaskInstance

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


def test_task_instance():
    return TaskInstance(test_task())


mock_resources = {
    "https://api.development.sinopia.io/resource/0000-1111-2222-3333": {
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
    },
    "https://api.development.sinopia.io/resource/4444-5555-6666-7777": {
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
    },
}

overlay_resources = [
    {
        "resource_uri": "https://api.development.sinopia.io/resource/0000-1111-2222-3333",
        "data": MARC_JSON,
    },
    {
        "resource_uri": "https://api.development.sinopia.io/resource/4444-5555-6666-7777",
        "data": MARC_JSON_NO_CAT_KEY,
    },
]

mock_push_store = {}


def mock_message():
    return [
        [
            {
                "Body": """{ "user": { "email": "dscully@stanford.edu"},
                            "resource": { "uri": "https://api.development.sinopia.io/resource/0000-1111-2222-3333" }}"""
            }
        ],
        [
            {
                "Body": """{ "user": { "email": "fmulder@stanford.edu"},
                            "resource": { "uri": "https://api.development.sinopia.io/resource/4444-5555-6666-7777" }}"""
            }
        ],
    ]


def marc_as_json():
    with open("tests/fixtures/record.json") as data:
        return data.read()


return_marc_tasks = [
    "process_symphony.convert_to_symphony_json",
    "process_symphony.marc_json_to_s3",
]


@pytest.fixture
def mock_task_instance(monkeypatch):
    def mock_xcom_pull(*args, **kwargs):
        key = kwargs.get("key")
        task_ids = kwargs.get("task_ids", [""])
        if key == "resources":
            return [
                "https://api.development.sinopia.io/resource/0000-1111-2222-3333",
                "https://api.development.sinopia.io/resource/4444-5555-6666-7777",
            ]
        elif key == "messages":
            return mock_message()
        elif key in mock_resources and task_ids[0] == "sqs-message-parse":
            return mock_resources[key]
        elif key == "overlay_resources":
            return overlay_resources
        elif task_ids[0] in return_marc_tasks:
            return marc_as_json()
        elif key == "new_resources":
            return mock_resources
        else:
            return mock_push_store[key]

    def mock_xcom_push(*args, **kwargs):
        key = kwargs.get("key")
        value = kwargs.get("value")
        mock_push_store[key] = value
        return None

    monkeypatch.setattr(TaskInstance, "xcom_pull", mock_xcom_pull)
    monkeypatch.setattr(TaskInstance, "xcom_push", mock_xcom_push)
