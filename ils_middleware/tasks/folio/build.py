import datetime
import logging

from folio_uuid import FOLIONamespaces, FolioUUID

from ils_middleware.tasks.folio.map import FOLIO_FIELDS

logger = logging.getLogger(__name__)


def _default_transform(**kwargs) -> tuple:
    folio_field = kwargs["folio_field"]
    values = kwargs.get("values", [])
    logger.debug(f"field: {folio_field} values: {values} type: {type(values)}")
    return folio_field, values


def _hrid(resource_uri: str, okapi_url: str) -> str:
    hrid = FolioUUID(okapi_url, FOLIONamespaces.instances, resource_uri.split("/")[-1])
    return str(hrid)


def _title_transform(**kwargs) -> tuple:
    values = kwargs.get("values")
    if isinstance(values, str):
        values = [
            [
                values,
            ]
        ]
    values = values[0]  # type: ignore
    number_fields = len(values)

    title = values[0]
    if number_fields > 1 and values[1]:  # subtitle
        title = f"{title} : {values[1]}"
    if number_fields > 2 and values[2]:  # partNumber"
        title = f"{title}. {values[2]}"
    if number_fields > 3 and values[3]:  # partName
        title = f"{title}, {values[3]}"
    return "title", title


def _user_folio_id(okapi_url: str, folio_user: str) -> str:
    folio_uuid = FolioUUID(okapi_url, FOLIONamespaces.users, folio_user)
    return str(folio_uuid)


transforms = {
    "title": _title_transform,
}


def _create_update_metadata(**kwargs) -> dict:
    okapi_url = kwargs["folio_url"]
    folio_user = kwargs["folio_login"]
    current_timestamp = datetime.datetime.utcnow().isoformat()
    user_uuid = _user_folio_id(okapi_url, folio_user)
    metadata = kwargs.get("metadata", {})
    if len(metadata) < 1:
        metadata = {
            "createdDate": current_timestamp,
            "createdByUserId": user_uuid,
        }
    else:
        metadata["updatedDate"] = current_timestamp
        metadata["updatedByUserId"] = user_uuid
    return metadata


def _task_ids(task_groups: str, folio_field: str) -> str:
    task_id = f"{folio_field}_task"
    if len(task_groups) > 0:
        task_id = f"{task_groups}.{task_id}"
    return task_id


def _inventory_record(**kwargs) -> dict:
    instance_uri = kwargs["instance_uri"]
    task_instance = kwargs["task_instance"]
    task_groups = ".".join(kwargs["task_groups_ids"])
    okapi_url = kwargs["folio_url"]
    folio_user = kwargs["folio_login"]

    record = {
        "hrid": _hrid(instance_uri, okapi_url),
        "metadata": _create_update_metadata(**kwargs),
    }
    for folio_field in FOLIO_FIELDS:
        post_processing = transforms.get(folio_field, _default_transform)
        task_id = _task_ids(task_groups, folio_field)
        raw_values = task_instance.xcom_pull(key=instance_uri, task_ids=task_id)
        if raw_values:
            record_field, values = post_processing(
                values=raw_values,
                okapi_url=okapi_url,
                folio_field=folio_field,
                folio_user=folio_user,
            )

            record[record_field] = values
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
