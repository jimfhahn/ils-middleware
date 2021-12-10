from tasks import test_task_instance  # noqa: F401
from tasks import mock_task_instance  # noqa: F401
from tasks import mock_requests_okapi  # noqa: F401


from ils_middleware.tasks.folio.new import post_folio_records

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
