import pytest
import requests  # type: ignore
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models.taskinstance import TaskInstance

from pytest_mock import MockerFixture

from ils_middleware.tasks.sinopia.update_resource import update_resource_new_metadata

from tasks import test_task_instance, mock_task_instance, mock_marc_as_json

SINOPIA_API = {
    "https://api.development.sinopia.io/resource/0000-1111-2222-3333": {
        "data": [
            {
                "@id": "https://s.io/resources/f3d34054",
                "@type": ["http://id.loc.gov/ontologies/bibframe/Instance"],
            }
        ],
        "bfAdminMetadataRefs": [],
    },
    "https://api.development.sinopia.io/resource/4444-5555-6666-7777": {
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

task_instance = test_task_instance()


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


def test_update_resource(mock_requests, mock_task_instance):
    update_resource_new_metadata(
        task_instance=task_instance,
        jwt="3445676",
    )

    assert len(task_instance.xcom_pull(key="updated_resources")) == 2
    assert len(task_instance.xcom_pull(key="resource_not_found")) == 0
