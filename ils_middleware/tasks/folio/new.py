import logging

import requests

logger = logging.getLogger(__name__)


def _post_to_okapi(**kwargs):
    endpoint = kwargs.get("endpoint", "/instance-storage/batch/synchronous")
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

    logging.error(f"Info {headers} {okapi_instance_url} {payload}")

    new_record_result = requests.post(
        okapi_instance_url,
        headers=headers,
        json=payload,
    )

    
    # Check to see if record already exists, if so try PUT.
    # When available should use upsert=true param to the original URI to
    # avoid this second check and operation
    if new_record_result.status_code == 422 and new_record.json().get("errors")[
        "message"
    ].startswith("id value already exists"):
        overlay_record_result = requests.put(
            okapi_instance_url,
            headers=headers,
            json=payload,
        )
        # overlay_record_result.raise_for_status()
    else:
        logger.error(f"New Record result {new_record_result.status_code}\nText:{new_record_result.text}")
        # new_record_result.raise_for_status()


def post_folio_records(**kwargs):
    """Creates new records in FOLIO"""
    task_instance = kwargs["task_instance"]
    jwt = kwargs["token"]
    task_groups = ".".join(kwargs["task_groups_ids"])


    resources = task_instance.xcom_pull(key="resources", task_ids="sqs-message-parse")

    inventory_records = []
    for instance_uri in resources:
        task_id = f"{task_groups}.build-folioIt"
        logger.error(f"Task id {task_id} for {instance_uri}")
        inventory_records.append(
            task_instance.xcom_pull(key=instance_uri, task_ids=task_id)
        )

    logging.error(f"BEFORE Submitting to Okapi {jwt}")
    _post_to_okapi(records=inventory_records, jwt=jwt, **kwargs)
    # Post back to
