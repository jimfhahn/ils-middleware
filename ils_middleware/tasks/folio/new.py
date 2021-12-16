"""Modules add new or existing inventory instance JSON records to FOLIO the
inventory-storage batch endpoint with upsert query parameter set to true.

See https://s3.amazonaws.com/foliodocs/api/mod-inventory-storage/p/instance-sync.html
"""
import logging

import requests

logger = logging.getLogger(__name__)


def _push_to_xcom(records: list, task_instance):
    for record in records:
        task_instance.xcom_push(key=record["hrid"], value=record["id"])


def _post_to_okapi(**kwargs):
    task_instance = kwargs["task_instance"]
    endpoint = kwargs.get("endpoint", "/instance-storage/batch/synchronous?upsert=true")
    jwt = kwargs["jwt"]
    records = kwargs["records"]
    tenant = kwargs["tenant"]
    okapi_url = kwargs["folio_url"]

    okapi_instance_url = f"{okapi_url}{endpoint}"

    headers = {
        "Content-type": "application/json",
        "user-agent": "Sinopia Airflow",
        "x-okapi-token": jwt,
        "x-okapi-tenant": tenant,
    }

    payload = {"instances": records}

    new_record_result = requests.post(
        okapi_instance_url,
        headers=headers,
        json=payload,
    )

    if new_record_result.status_code < 300:
        _push_to_xcom(records, task_instance)
    else:
        logger.error(f"New records failed errors: {new_record_result.json()}")
        new_record_result.raise_for_status()


def post_folio_records(**kwargs):
    """Creates new records in FOLIO"""
    task_instance = kwargs["task_instance"]
    jwt = kwargs["token"]
    task_groups = ".".join(kwargs["task_groups_ids"])

    task_id = "build-folio"
    if len(task_groups) > 0:
        task_id = f"{task_groups}.{task_id}"

    resources = task_instance.xcom_pull(key="resources", task_ids="sqs-message-parse")

    inventory_records = []
    for instance_uri in resources:
        inventory_records.append(
            task_instance.xcom_pull(key=instance_uri, task_ids=task_id)
        )

    _post_to_okapi(records=inventory_records, jwt=jwt, **kwargs)
