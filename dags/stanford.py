import logging

from datetime import datetime, timedelta

from aws_sns import SubscribeOperator
from folio import map_to_folio
from sinopia import UpdateIdentifier, GitRdf2Marc, Rdf2Marc


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator


def choose_ils(**kwargs) -> str:
    import random

    ils_choice = random.random()
    logging.info(f"Random ILS Choice {ils_choice}")
    if ils_choice <= 0.50:
        return "git_rdf2marc"
    return "folio_map"


def retrive_rdf2marc(**kwargs) -> str:
    logging.info(f"Retrieves MARC file from rdf2marc {kwargs}")
    return "update_sinopia"


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
    "stanford",
    default_args=default_args,
    description="Stanford Symphony and FOLIO DAG",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2021, 8, 24),
    tags=["symphony", "folio"],
    catchup=False,
) as dag:
    # Monitors SNS for Stanford topic
    listen_sns = SubscribeOperator(topic="stanford")

    urls = [
        "http://sinopia-example.io/stanford/01",
        "http://sinopia-example.io/stanford/02",
        "http://sinopia-example.io/stanford/03",
    ]

    branch_ils = BranchPythonOperator(task_id="ils", python_callable=choose_ils)

    # Retrive MARC from Sinopia API, convert to MARC JSON,
    setup_rdf2marc = GitRdf2Marc()

    run_rdf2marc = Rdf2Marc(instance_url=urls[0])

    sinopia_to_symphony_json = PythonOperator(
        task_id="symphony_json",
        python_callable=retrive_rdf2marc,
        op_kwargs={"urls": urls},
    )

    connect_symphony_cmd = """echo send POST to Symphony Web Services, returns CATKEY
    exit 0"""

    #  Send to Symphony Web API
    send_to_symphony = BashOperator(
        task_id="symphony_send", bash_command=connect_symphony_cmd
    )

    sinopia_to_folio_records = PythonOperator(
        task_id="folio_map", python_callable=map_to_folio, op_kwargs={"urls": urls}
    )

    connect_okapi_cmd = """echo POST to Okapi's /inventory/items with map_sinopia_to_inventory_records results
    exit 0"""

    send_to_folio = BashOperator(task_id="folio_send", bash_command=connect_okapi_cmd)

    # Dummy Operator
    processed_sinopia = DummyOperator(
        task_id="processed_sinopia", dag=dag, trigger_rule="none_failed"
    )
    # Updates Sinopia URLS with HRID
    update_sinopia = UpdateIdentifier(urls=urls)

listen_sns >> branch_ils >> [setup_rdf2marc, sinopia_to_folio_records]
(
    setup_rdf2marc
    >> run_rdf2marc
    >> sinopia_to_symphony_json
    >> send_to_symphony
    >> processed_sinopia
)
sinopia_to_folio_records >> send_to_folio >> processed_sinopia
processed_sinopia >> update_sinopia
