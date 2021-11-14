import pytest
import requests  # type: ignore
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models.taskinstance import TaskInstance

from pytest_mock import MockerFixture

from ils_middleware.tasks.sinopia.update_resource import update_resource_new_metadata

SINOPIA_API = {
    "http://example.com/rdf/0000-1111-2222-3333": {
        "data": [
            {
                "@id": "https://s.io/resources/f3d34054",
                "@type": ["http://id.loc.gov/ontologies/bibframe/Instance"],
            }
        ],
        "bfAdminMetadataRefs": [],
    },
    "http://example.com/rdf/4444-5555-6666-7777": {
        "data": [
            {
                "@id": "https://s.io/resources/acde48001122",
                "@type": ["http://id.loc.gov/ontologies/bibframe/Instance"],
            }
        ],
        "bfAdminMetadataRefs": [],
    },
}

SINOPIA_API_ERRORS = ["https://s.io/resources/acde48001122"]


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
def mock_requests(monkeypatch, mocker: MockerFixture):
    def mock_get(*args, **kwargs):
        get_response = mocker.stub(name="get_result")
        get_response.status_code = 200
        resource_uri = args[0]
        if resource_uri in SINOPIA_API:
            get_response.json = lambda: SINOPIA_API.get(resource_uri)
        else:
            get_response.status_code = 404
            get_response.text = "Not found"
        return get_response

    def mock_put(*args, **kwargs):
        put_response = mocker.stub(name="put_result")
        put_response.status_code = 201
        resource_uri = args[0]
        if resource_uri in SINOPIA_API_ERRORS:
            put_response.status_code = 500
            put_response.text = "Server error"

        return put_response

    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.setattr(requests, "put", mock_put)


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
def mock_resources(mock_resource):
    return [
        {
            "resource_uri": "http://example.com/rdf/0000-1111-2222-3333",
            "metadata_uri": "https://api.development.sinopia.io/resource/1a3cebda-34b9-4e15-bc79-f6a5f915ce76",
            "resource": mock_resource
        },
        {
            "resource_uri": "http://example.com/rdf/4444-5555-6666-7777",
            "metadata_uri": "https://api.development.sinopia.io/resource/1a3cebda-34b9-4e15-bc79-f6a5f915ce76",
            "resource": mock_resource
        },
        {
            "resource_uri": "https://s.io/resources/11ec",
            "metadata_uri": "https://s.io/resources/b5db",
        }
    ]


@pytest.fixture
def mock_task_instance(mock_resources, monkeypatch): # , mock_resources):
    def mock_xcom_pull(*args, **kwargs):
        key = kwargs.get("key")
        task_ids = kwargs.get("task_ids")
        if key == "resources":
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


def test_update_resource(mock_requests, mock_task_instance):
    result = update_resource_new_metadata(
        task_instance=task_instance,
        jwt="3445676",
    )

    assert len(task_instance.xcom_pull(key="updated_resources")) == 2
    assert len(task_instance.xcom_pull(key="resource_not_found")) == 1


# def test_missing_resource_uri(mock_requests, mock_task_instance):
#     with pytest.raises(
#         Exception, match="https://s.io/resources/11ec retrieval error 404"
#     ):
#         update_resource_new_metadata(
#             jwt="33446",
#             resource_uri="https://s.io/resources/11ec",
#             metadata_uri="https://s.io/resources/b5db",
#         )


# def test_bad_put_call_for_resource(mock_requests, mock_task_instance):
#     with pytest.raises(
#         Exception,
#         match="Failed to update https://s.io/resources/acde48001122, status code 500",
#     ):
#         update_resource_new_metadata(
#             jwt="5677sce",
#             resource_uri="https://s.io/resources/acde48001122",
#             metadata_uri="https://s.io/resources/7a669b8c",
#         )
