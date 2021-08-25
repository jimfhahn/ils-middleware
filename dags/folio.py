import logging
from datetime import datetime, timedelta

from aws_sns import SubscribeOperator
from sinopia import UpdateIdentifier

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def map_sinopia_to_folio(urls: list):
    logging.info(f"Starting with {len(urls)} from Sinopia")
    for url in urls:
        logging.info(f"Sinopia {url} retrives Document")
    logging.info(f"Finished with {len(urls)} from Sinopi")
    logging.info("Magic Mapping to FOLIO happens here")
    return "folio_send"


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
    "folio",
    default_args=default_args,
    description="FOLIO DAG",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2021, 8, 14),
    tags=["folio"],
    catchup=False,
) as dag:

    # Monitors SNS for Cornell topic
    listen_sns = SubscribeOperator(topic="cornell")

    # Should retrieve URLS from a listen_sns_task task message
    urls = ["http://sinopia-example.io/{{dag_run.conf.get('name')}}/01",
            "http://sinopia-example.io/{{dag_run.conf.get('name')}}/02",
            "http://sinopia-example.io/{{dag_run.conf.get('name')}}/03"]

    # Maps Documents from URLs in the SNS Message to FOLIO JSON
    map_sinopia_to_inventory_records = PythonOperator(
        task_id="folio_map",
        python_callable=map_sinopia_to_folio,
        op_kwargs={'urls': urls}
    )

    connect_okapi_cmd = """echo POST to Okapi's /inventory/items with map_sinopia_to_inventory_records results
    exit 0"""

    send_to_folio = BashOperator(
        task_id="folio_send",
        # task_id="folio_send",
        bash_command=connect_okapi_cmd
    )

    # Updates Sinopia URLS with HRID
    update_sinopia =  UpdateIdentifier(urls=urls)

listen_sns >> map_sinopia_to_inventory_records >> send_to_folio  >> update_sinopia
