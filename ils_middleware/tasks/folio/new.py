import logging

import requests

logger = logging.getLogger(__name__)


def _post_to_okapi(**kwargs):
    endpoint = kwargs["endpoint"]
    jwt = kwargs["jwt"]
    record = kwargs["record"]
    tenant = kwargs["tenant"]
    okapi_url = kwargs["folio_url"]

    okapi_post_url = f"{okapi_url}{endpoint}"

    headers = {
        "Content-type": "application/json",
        "x-okapi-token": jwt,
        "x-okapi-tenant": tenant,
    }

    new_record_result = requests.post(
        okapi_post_url,
        headers=headers,
        json=record,
    )

    new_record_result.raise_for_status()


def post_folio_records(**kwargs):
    """Creates new records in FOLIO"""
    task_instance = kwargs["task_instance"]

    resources = task_instance.xcom_pull(key="resources", task_ids="sqs-message-parse")
    jwt = task_instance.xcom_pull(key="folio-login")
    for instance_uri in resources:
        folio_record = task_instance.xcom_pull(
            key=instance_uri, task_ids="folio-inventory-record"
        )
        _post_to_okapi(record=folio_record, jwt=jwt, **kwargs)
