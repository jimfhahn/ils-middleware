from tasks import test_task_instance  # noqa: F401
from tasks import mock_task_instance  # noqa: F401
from tasks import mock_requests_okapi  # noqa: F401


from ils_middleware.tasks.folio.new import post_folio_records

okapi_uri = "https://okapi-folio.dev.edu/"


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
