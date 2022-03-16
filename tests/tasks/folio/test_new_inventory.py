import pytest
import requests

from pytest_mock import MockerFixture

from tasks import test_task_instance  # noqa: F401
from tasks import mock_task_instance  # noqa: F401
from tasks import mock_requests_okapi  # noqa: F401


from ils_middleware.tasks.folio.new import post_folio_records, logger

okapi_uri = "https://okapi-folio.dev.edu/"
instance_uri = "https://api.development.sinopia.io/resource/0000-1111-2222-3333"


def test_happypath_post_folio_record(
    mock_task_instance, mock_requests_okapi  # noqa: F811
):
    post_folio_records(
        task_instance=test_task_instance(),
        tenant="sul",
        endpoint="/instance-storage/instances",
        folio_url=okapi_uri,
        folio_login="test_user",
        token="asdfsadfcedd",
        task_groups_ids=[""],
    )

    assert (
        test_task_instance().xcom_pull(key=instance_uri)
    ) == "147b1171-740e-513e-84d5-b63a9642792c"


@pytest.fixture
def mock_bad_request(monkeypatch, mocker: MockerFixture):
    def mock_raise_for_status(*args, **kwargs):
        error_response = mocker.stub(name="post_error")
        error_response.status_code = 500
        error_response.text = "Internal server error"
        raise requests.HTTPError(error_response.status_code, error_response.text)

    def mock_bad_post(*args, **kwargs):
        error_message = {"errors": [{"message": "id value already exists"}]}
        post_response = mocker.stub(name="post_result")
        post_response.status_code = 422
        post_response.json = lambda: error_message
        post_response.text = str(error_message)
        post_response.raise_for_status = mock_raise_for_status
        return post_response

    monkeypatch.setattr(requests, "post", mock_bad_post)
    # monkeypatch.setattr(requests.Response, "raise_for_status", mock_raise_for_status)


def test_raised_error(
    mock_task_instance, mock_bad_request, mocker: MockerFixture  # noqa: F811
):
    logger_spy = mocker.spy(logger, "error")

    with pytest.raises(requests.HTTPError, match="Internal server error"):
        post_folio_records(
            task_instance=test_task_instance(),
            tenant="sul",
            endpoint="/instance-storage/batch/instances?upsert=true",
            folio_url=okapi_uri,
            folio_login="test_user",
            token="asdfsadfcedd",
            task_groups_ids=["folio"],
        )

    logger_spy.assert_called_once_with(
        "New records failed errors: {'errors': [{'message': 'id value already exists'}]}"
    )
