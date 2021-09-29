"""DAG for Cornell University Libraries."""
import logging
from datetime import datetime, timedelta

from folio import map_to_folio
from tasks.amazon.sqs import SubscribeOperator
from tasks.sinopia.sinopia import UpdateIdentifier
from folio_request import FolioRequest
from folio_login import FolioLogin

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

    send_to_folio = FolioRequest(
        tenant="cornell",
        token=FolioLogin(tenant="cornell", username="", password=""),
        endpoint="",
    )

    # Updates Sinopia URLS with HRID
    update_sinopia = UpdateIdentifier(urls=[], identifier="borked:1")

listen_sns >> map_sinopia_to_inventory_records >> send_to_folio >> update_sinopia
