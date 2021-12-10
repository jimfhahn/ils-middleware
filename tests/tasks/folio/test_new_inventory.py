import pytest
import requests

from pytest_mock import MockerFixture

from tasks import test_task_instance  # noqa: F401
from tasks import mock_task_instance  # noqa: F401
from tasks import mock_requests_okapi  # noqa: F401


from ils_middleware.tasks.folio.new import post_folio_records, _overlay_folio_records

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


def test_overlay_folio_records(mock_task_instance, mock_requests_okapi):  # noqa: F811
    assert (_overlay_folio_records(okapi_uri, headers={}, payload={})) is None


@pytest.fixture
def mock_bad_request(monkeypatch, mocker: MockerFixture):
    def mock_bad_post(*args, **kwargs):
        post_response = mocker.stub(name="post_result")
        post_response.status_code = 422
        post_response.json = lambda: {
            "errors": [{"message": "id value already exists"}]
        }
        return post_response

    def mock_put(*args, **kwargs):
        put_response = mocker.stub(name="put_result")
        put_response.status_code = 422
        put_response.text = "[{}]"
        put_response.raise_for_status = lambda: None
        return put_response

    monkeypatch.setattr(requests, "post", mock_bad_post)
    monkeypatch.setattr(requests, "put", mock_put)


def test_error_message(mock_task_instance, mock_bad_request):  # noqa: F811
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
