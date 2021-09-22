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
from airflow.operators.dummy_operator import DummyOperator
<<<<<<< HEAD
from airflow.operators.python import PythonOperator
=======
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook


def choose_ils(**kwargs) -> str:
    import random

    ils_choice = random.random()
    logging.info(f"Random ILS Choice {ils_choice}")
    if ils_choice <= 0.50:
        return "symphony_json"
    return "symphony_json"  # return "folio_map"
>>>>>>> fabab16 (Use an AwsLambdaHook to trigger the rdf2marc lambda)


def retrive_rdf2marc(**kwargs) -> str:
    """Stub function for processing MARC file from running rdf2marc."""
    logging.info(f"Retrieves MARC file from rdf2marc {kwargs}")
    lambda_hook = AwsLambdaHook(
        'sinopia-rdf2marc-development',
        log_type='None',
        qualifier='$LATEST',
        invocation_type='RequestResponse',
        config=None,
        aws_conn_id='aws_lambda_connection'
    )
    logging.info(f"Hook = {lambda_hook}")
    payload = """
    {"instance_uri": "https://api.stage.sinopia.io/resource/3eb5f480-60d6-4732-8daf-02d8d6f26eb7",
     "bucket": "sinopia-marc-development",
     "marc_path": "marc/airflow/3eb5f480-60d6-4732-8daf-02d8d6f26eb7/record.mar",
     "marc_txt_path": "marc/airflow/3eb5f480-60d6-4732-8daf-02d8d6f26eb7/record.txt",
     "error_path": "marc/airflow/3eb5f480-60d6-4732-8daf-02d8d6f26eb7/error.txt"
    }"""
    return lambda_hook.invoke_lambda(payload=payload)


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
    # setup_rdf2marc = GitRdf2Marc()

<<<<<<< HEAD
    run_rdf2marc = Rdf2Marc(instance_url="")
=======
    # run_rdf2marc = Rdf2Marc(instance_url=urls[0])
>>>>>>> fabab16 (Use an AwsLambdaHook to trigger the rdf2marc lambda)

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
<<<<<<< HEAD
    update_sinopia = UpdateIdentifier(urls=[])

messages >> [setup_rdf2marc, sinopia_to_folio_records]
(setup_rdf2marc >> run_rdf2marc >> sinopia_to_symphony_json >> processed_sinopia)
=======
    update_sinopia = UpdateIdentifier(urls=urls)

listen_sns >> branch_ils >> [sinopia_to_symphony_json, sinopia_to_folio_records]
(
    # setup_rdf2marc
    # >> run_rdf2marc
    sinopia_to_symphony_json
    >> send_to_symphony
    >> processed_sinopia
)
>>>>>>> fabab16 (Use an AwsLambdaHook to trigger the rdf2marc lambda)
sinopia_to_folio_records >> send_to_folio >> processed_sinopia
processed_sinopia >> update_sinopia
