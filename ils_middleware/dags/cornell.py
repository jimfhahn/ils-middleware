"""DAG for Cornell University Libraries."""
import logging
from datetime import datetime, timedelta

from ils_middleware.tasks.folio.map import map_to_folio
from ils_middleware.tasks.amazon.sqs import SubscribeOperator
from ils_middleware.tasks.sinopia.local_metadata import new_local_admin_metadata
from ils_middleware.tasks.sinopia.login import sinopia_login
from ils_middleware.tasks.folio.request import FolioRequest
from ils_middleware.tasks.folio.login import FolioLogin

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "provider": None,
}

with DAG(
    "cornell",
    default_args=default_args,
    description="Cornell FOLIO DAG",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2021, 8, 14),
    tags=["folio"],
    catchup=False,
) as dag:

    # Monitors SNS for Cornell topic
    listen_sns = SubscribeOperator(queue="cornell-ils")

    # Maps Documents from URLs in the SNS Message to FOLIO JSON
    map_sinopia_to_inventory_records = PythonOperator(
        task_id="folio_map",
        python_callable=map_to_folio,
        op_kwargs={"url": "http://example-instance.sinopia.io"},
    )

    logging.info(
        "POST to Okapi's /inventory/items with map_sinopia_to_inventory_records result"
    )
    connect_okapi_cmd = """exit 0"""

    folio_login = FolioLogin(tenant="cornell", username="", password="")

    send_to_folio = FolioRequest(
        task_id="cornell_send_to_folio",
        tenant="cornell",
        token="{{ task_instance.xcom_pull(key='return_value', task_ids=['folio_login'])[0]}}",
        endpoint="",
    )

    # Sinopia Login
    login_sinopia = PythonOperator(
        task_id="sinopia-login",
        python_callable=sinopia_login,
        op_kwargs={
            "region": "us-west-1",
            "sinopia_env": "dev",
        },
    )

    # Adds localAdminMetadata
    local_admin_metadata = PythonOperator(
        task_id="sinopia-new-metadata",
        python_callable=new_local_admin_metadata,
        op_kwargs={
            "jwt": "{{ task_instance.xcom_pull(task_ids='update_sinopia.sinopia-login', key='return_value') }}",
            "group": "{{ task_instance.xcom_pull(task_ids='sqs-message-parse', key='group') }}",
            "instance_uri": "{{ task_instance.xcom_pull(task_ids='sqs-message-parse', key='resource_uri') }}",
            "ils_identifiers": {
                "folio": "{{ task_instance.xcom_pull(task_ids='send_to_folio', key='return_value') }}"
            },
        },
    )
listen_sns >> map_sinopia_to_inventory_records >> login_sinopia >> local_admin_metadata
