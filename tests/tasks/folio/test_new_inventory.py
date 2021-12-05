import pytest
import requests

from pytest_mock import MockerFixture

from tasks import test_task_instance, mock_task_instance  # noqa: F401

from ils_middleware.tasks.folio.new import post_folio_records

okapi_uri = "https://okapi-folio.dev.edu/"


@pytest.fixture
def mock_requests_okapi(monkeypatch, mocker: MockerFixture):
    def mock_post(*args, **kwargs):
        post_response = mocker.stub(name="post_result")
        post_response.status_code = 201
        post_response.headers = {"x-okapi-token": "some_jwt_token"}
        post_response.raise_for_status = lambda: None

        return post_response

    def mock_raise_for_status(*args, **kwargs):
        error_response = mocker.stub(name="post_error")
        error_response.status_code = 500

    monkeypatch.setattr(requests, "post", mock_post)
    monkeypatch.setattr(requests.Response, "raise_for_status", mock_raise_for_status)


def test_happypath_post_folio_record(
    mock_task_instance, mock_requests_okapi  # noqa: F811
):
    post_folio_records(
        task_instance=test_task_instance(),
        tenant="sul",
        endpoint="/instance-storage/instances",
        folio_url=okapi_uri,
        folio_login="test_user",
    )
