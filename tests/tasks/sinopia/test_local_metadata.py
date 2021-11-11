"""Test Sinopia LocalAdminMetadata"""
import uuid

import pytest
import rdflib
import requests  # type: ignore

from airflow.models import Variable
from pytest_mock import MockerFixture

from ils_middleware.tasks.sinopia.local_metadata import (
    create_admin_metadata,
    new_local_admin_metadata,
)


@pytest.fixture
def mock_requests_post(monkeypatch, mocker: MockerFixture):
    mock_request = mocker.stub(name="mock_post")
    mock_request.text = "https://sinopia.io/resource/4546abcd890"
    mock_request.status_code = 200

    def mock_post(*args, **kwargs):
        return mock_request

    monkeypatch.setattr(requests, "post", mock_post)


@pytest.fixture
def mock_airflow_variables(monkeypatch):
    def mock_get(*args, **kwargs):
        if args[0].startswith("sinopia_user"):
            return "ils_middleware"
        if args[0].startswith("dev_sinopia_api_uri"):
            return "https://api.development.sinopia.io/resource"

    monkeypatch.setattr(Variable, "get", mock_get)


@pytest.fixture
def mock_uuid(monkeypatch):
    def mock_uuid4(*args, **kwargs):
        return "1a3cebda-34b9-4e15-bc79-f6a5f915ce76"

    monkeypatch.setattr(uuid, "uuid4", mock_uuid4)


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


def test_new_local_admin_metadata(
    mock_requests_post, mock_airflow_variables, mock_uuid, mock_resource
):
    local_admin_metadata_uri = new_local_admin_metadata(
        jwt="abcd1234efg",
        resource=str(mock_resource),
        instance_uri="https://api.development.sinopia.io/resource/tyu8889asdf",
    )

    assert (
        local_admin_metadata_uri
        == "https://api.development.sinopia.io/resource/1a3cebda-34b9-4e15-bc79-f6a5f915ce76"
    )


def test_create_admin_metadata():
    admin_metadata = rdflib.Graph()
    admin_metadata_str = create_admin_metadata(
        instance_uri="https://api.sinopia.io/resource/12345",
        cataloger_id="ils_middleware",
        ils_identifiers={"symphony": "234566"},
    )
    admin_metadata.parse(data=admin_metadata_str, format="json-ld")
    assert len(admin_metadata) == 11
    for row in admin_metadata.query(
        """SELECT ?ident WHERE {
       ?id rdf:type <http://id.loc.gov/ontologies/bibframe/Local> .
       ?id rdf:value ?ident .
    }"""
    ):
        assert str(row[0]) == "234566"
