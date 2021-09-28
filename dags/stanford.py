"""DAG for Stanford University Libraries."""
import logging

from datetime import datetime, timedelta

from aws_sqs import SubscribeOperator
from folio import map_to_folio
from sinopia import UpdateIdentifier, GitRdf2Marc, Rdf2Marc
from folio_request import FolioRequest
from folio_login import FolioLogin
# from symphony import

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator


def retrive_rdf2marc(**kwargs) -> str:
    """Stub function for processing MARC file from running rdf2marc."""
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
    schedule_interval=timedelta(minutes=2),
    start_date=datetime(2021, 8, 24),
    tags=["symphony", "folio"],
    catchup=False,
) as dag:
    messages = SubscribeOperator(queue="stanford-ils")

    # Retrive MARC from Sinopia API, convert to MARC JSON,
    setup_rdf2marc = GitRdf2Marc()

    run_rdf2marc = Rdf2Marc(instance_url="")

    sinopia_to_symphony_json = PythonOperator(
        task_id="symphony_json",
        python_callable=retrive_rdf2marc,
        op_kwargs={"urls": []},
    )

    sinopia_to_folio_records = PythonOperator(
        task_id="folio_map", python_callable=map_to_folio, op_kwargs={"urls": []}
    )

    send_to_folio = FolioRequest(
        tenant="sul",
        token=FolioLogin(tenant="sul", username="", password=""),
        endpoint="",
    )
    # Dummy Operator
    processed_sinopia = DummyOperator(
        task_id="processed_sinopia", dag=dag, trigger_rule="none_failed"
    )
    # Updates Sinopia URLS with HRID
    update_sinopia = UpdateIdentifier(urls=[])

messages >> [setup_rdf2marc, sinopia_to_folio_records]
(setup_rdf2marc >> run_rdf2marc >> sinopia_to_symphony_json >> processed_sinopia)
sinopia_to_folio_records >> send_to_folio >> processed_sinopia
processed_sinopia >> update_sinopia
