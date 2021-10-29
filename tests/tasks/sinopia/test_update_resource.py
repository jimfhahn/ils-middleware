import pytest
import requests  # type: ignore

from pytest_mock import MockerFixture

from ils_middleware.tasks.sinopia.update_resource import update_resource_new_metadata

SINOPIA_API = {
    "https://s.io/resources/f3d34054": {
        "data": [
            {
                "@id": "https://s.io/resources/f3d34054",
                "@type": ["http://id.loc.gov/ontologies/bibframe/Instance"],
            }
        ],
        "bfAdminMetadataRefs": [],
    },
    "https://s.io/resources/acde48001122": {
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


def test_successful_update_resource(mock_requests):
    result = update_resource_new_metadata(
        jwt="3445676",
        resource_uri="https://s.io/resources/f3d34054",
        metadata_uri="https://s.io/resources/38eb",
    )

    assert result.startswith("resource_updated")


def test_missing_resource_uri(mock_requests):
    with pytest.raises(
        Exception, match="https://s.io/resources/11ec retrieval error 404"
    ):
        update_resource_new_metadata(
            jwt="33446",
            resource_uri="https://s.io/resources/11ec",
            metadata_uri="https://s.io/resources/b5db",
        )


def test_bad_put_call_for_resource(mock_requests):
    with pytest.raises(
        Exception,
        match="Failed to update https://s.io/resources/acde48001122, status code 500",
    ):
        update_resource_new_metadata(
            jwt="5677sce",
            resource_uri="https://s.io/resources/acde48001122",
            metadata_uri="https://s.io/resources/7a669b8c",
        )
