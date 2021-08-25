import logging
from datetime import datetime, timedelta

from aws_sns import SubscribeOperator
from folio import map_to_folio
from sinopia import UpdateIdentifier

from airflow import DAG
from airflow.operators.bash import BashOperator
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
    listen_sns = SubscribeOperator(topic="cornell")

    # Should retrieve URLS from a listen_sns_task task message
    urls = [
        "http://sinopia-example.io/cornell/01",
        "http://sinopia-example.io/cornell/02",
        "http://sinopia-example.io/cornell/03",
    ]

    # Maps Documents from URLs in the SNS Message to FOLIO JSON
    map_sinopia_to_inventory_records = PythonOperator(
        task_id="folio_map", python_callable=map_to_folio, op_kwargs={"urls": urls}
    )

    logging.info(
        "POST to Okapi's /inventory/items with map_sinopia_to_inventory_records result"
    )
    connect_okapi_cmd = """exit 0"""

    send_to_folio = BashOperator(
        task_id="cornell_folio_send",
        # task_id="folio_send",
        bash_command=connect_okapi_cmd,
    )

    # Updates Sinopia URLS with HRID
    update_sinopia = UpdateIdentifier(urls=urls, identifier="borked:1")

listen_sns >> map_sinopia_to_inventory_records >> send_to_folio >> update_sinopia
