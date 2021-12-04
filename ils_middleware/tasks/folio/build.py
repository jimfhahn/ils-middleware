import datetime
import logging

from airflow.models import Variable
from folio_uuid import FOLIONamespaces, FolioUUID

from ils_middleware.tasks.folio.map import FOLIO_FIELDS

logger = logging.getLogger(__name__)


def _default_transform(**kwargs) -> str:
    folio_field = kwargs["folio_field"]
    values = kwargs.get("values", [])
    logger.debug(f"field: {folio_field} values: {values} type: {type(values)}")
    if isinstance(values, list):
        values = ["".join(row) for row in values]
    else:
        values = "".join(values)
    return folio_field, values


def _hrid(resource_uri: str, okapi_url: str) -> str:
    hrid = FolioUUID(okapi_url, FOLIONamespaces.instances, resource_uri.split("/")[-1])
    return str(hrid)


def _title_transform(**kwargs) -> str:
    values = kwargs.get("values")
    for row in values:
        title = row[0]  # mainTitle
        if row[1]:  # subtitle
            title = f"{title} : {row[1]}"
        if row[2]:  # partNumber"
            row = f"{title}. {row[2]}"
        if row[3]:  # partName
            row = f"{title}, {row[3]}"
    return "title", title


def _user_folio_id(okapi_url: str, folio_user: str) -> str:
    folio_uuid = FolioUUID(okapi_url, FOLIONamespaces.users, folio_user)
    return str(folio_uuid)


transforms = {
    "title": _title_transform,
}


def _inventory_record(**kwargs) -> dict:
    instance_uri = kwargs["instance_uri"]
    task_instance = kwargs["task_instance"]
    task_groups = ".".join(kwargs["task_groups_ids"])
    okapi_url = kwargs["folio_url"]
    folio_user = kwargs["folio_login"]

    record = {"hrid": _hrid(instance_uri, okapi_url)}


    record["metadata"] = kwargs.get("metadata")

    if not record["metadata"]:
        record["metadata"] = {
            "createdDate": datetime.datetime.utcnow().isoformat(),
            "createdByUserId": _user_folio_id(okapi_url, folio_user),
        }

    for folio_field in FOLIO_FIELDS:
        post_processing = transforms.get(folio_field, _default_transform)
        task_id = f"{task_groups}.{folio_field}_task"
        raw_values = task_instance.xcom_pull(key=instance_uri, task_ids=task_id)
        if raw_values:
            record_field, values = post_processing(
                values=raw_values,
                okapi_url=okapi_url,
                folio_field=folio_field,
                folio_user=folio_user
            )
            record[record_field] = values
        else:
            logger.debug(f"{raw_values} values for {instance_uri}'s {task_id}")
    return record


def build_records(**kwargs):
    """ """
    task_instance = kwargs["task_instance"]

    resources = task_instance.xcom_pull(key="resources", task_ids="sqs-message-parse")
    for resource_uri in resources:
        inventory_rec = _inventory_record(
            instance_uri=resource_uri,
            **kwargs,
        )
        task_instance.xcom_push(key=resource_uri, value=inventory_rec)
    return "build-complete"
