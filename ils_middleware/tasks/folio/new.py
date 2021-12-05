import requests

from airflow.models import Variable

def _post_to_okapi(**kwargs):
    endpoint = kwargs.get("endpoint")
    jwt = kwargs.get("jwt")
    record = kwargs["record"]
    tenant = kwargs["tenant"]
    okapi_url = kwargs["okapi_url"]

    okapi_post_url = f"{okapi_url}{endpoint}" 

    headers = {"Content-type": "application/json",
               "x-okapi-token": jwt,
               "x-okapi-tenant": tenant}

    new_record_result = requests.post(okapi_post_url,
        headers=headers,
        json=record,
    )

    new_record_result.raise_for_status()


def post_folio_record(**kwargs):
    """Creates a new record in FOLIO"""
    task_instance = kwargs.get("task_instance")

    resources = task_instance.xcom_pull(key="resources", task_ids="sqs-message-parse")
    jwt = task_instance.xcom_pull(key="folio-login")
    for instance_uri in resources:
        folio_record = task_instance(key=instance_uri, task_ids="folio-inventory-record")
        _post_to_okapi(folio_record, **kwargs)


