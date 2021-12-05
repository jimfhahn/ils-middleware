import datetime

from unittest.mock import MagicMock

import pytest

from tasks import test_task_instance, mock_task_instance  # noqa: F401

from ils_middleware.tasks.folio.build import (
    build_records,
    _default_transform,
    _hrid,
    _inventory_record,
    _title_transform,
    _user_folio_id,
)

instance_uri = "https://api.development.sinopia.io/resource/0000-1111-2222-3333"
okapi_uri = "https://okapi-folio.dev.edu/"


@pytest.fixture
def mock_variable(monkeypatch):
    datetime_mock = MagicMock(wrap=datetime.datetime)
    datetime_mock.isoformat.return_value = "2021-12-06T15:30:28.000124"

    monkeypatch.setattr(datetime, "datetime", datetime_mock)


def test_happypath_build_records(mock_task_instance):  # noqa: F811

    build_records(
        task_instance=test_task_instance(),
        task_groups_ids=[],
        folio_url=okapi_uri,
        folio_login="test_user",
    )
    record = test_task_instance().xcom_pull(
        key="https://api.development.sinopia.io/resource/0000-1111-2222-3333"
    )

    assert record["hrid"].startswith("3d9cbd7a-dca8-5c57-a13e-0102d9539814")
    assert record["metadata"]["createdByUserId"].startswith(
        "acd2f97b-1061-549e-a543-a96e89605f6f"
    )
    assert record["title"] == "Great force"


def test_default_transform_value_listing():
    folio_field = "contributor.Primary"
    name = "Butler, Octavia"
    default_tuple = _default_transform(
        folio_field=folio_field,
        values=[
            [
                name,
            ],
        ],
    )

    assert default_tuple[0].startswith(folio_field)
    assert name in default_tuple[1][0]


def test_default_hrid():
    hrid = _hrid(instance_uri, okapi_uri)
    assert hrid.startswith("3d9cbd7a-dca8-5c57-a13e-0102d9539814")


def test_inventory_record(mock_task_instance):  # noqa: F811
    record = _inventory_record(
        instance_uri=instance_uri,
        task_instance=test_task_instance(),
        task_groups_ids=[""],
        folio_url=okapi_uri,
        folio_login="test_user",
    )
    assert record["hrid"].startswith("3d9cbd7a-dca8-5c57-a13e-0102d9539814")


def test_inventory_record_existing_metadata(mock_task_instance):  # noqa: F811
    metadata = {
        "createdDate": "2021-12-06T15:45:28.140795",
        "createdByUserId": "9b80f3af-a07a-5e6a-a5fb-3d5723ea94de",
    }
    record = _inventory_record(
        instance_uri=instance_uri,
        task_instance=test_task_instance(),
        task_groups_ids=["folio"],
        folio_url=okapi_uri,
        folio_login="test_user",
        metadata=metadata,
    )
    assert record["hrid"].startswith("3d9cbd7a-dca8-5c57-a13e-0102d9539814")
    assert record["metadata"]["createdDate"].startswith("2021-12-06T15:45:28.140795")


def test_inventory_record_no_values():
    with pytest.raises(KeyError, match="instance_uri"):
        _inventory_record()


def test_title_transform_all():
    title_tuple = _title_transform(
        values=[["COVID-19", "Survivors", "California", "1st"]]
    )
    assert title_tuple[0].startswith("title")
    assert title_tuple[1].startswith("COVID-19 : Survivors. California, 1st")


def test_title_none_parts():
    title_tuple = _title_transform(values=[["COVID-19", None, None, None]])
    assert title_tuple[0].startswith("title")
    assert title_tuple[1].startswith("COVID-19")


def test_folio_uuid():
    user_uuid = _user_folio_id(okapi_uri, "dschully")

    assert user_uuid.startswith("9b80f3af-a07a-5e6a-a5fb-3d5723ea94de")
