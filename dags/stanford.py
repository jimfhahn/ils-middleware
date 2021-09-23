import logging
import os

from datetime import datetime, timedelta

from aws_sns import SubscribeOperator
from folio import map_to_folio
from sinopia import UpdateIdentifier, Rdf2Marc
from pymarc import MARCReader

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def choose_ils(**kwargs) -> str:
    import random

    ils_choice = random.random()
    logging.info(f"Random ILS Choice {ils_choice}")
    if ils_choice <= 0.50:
        return "symphony_json"
    return "symphony_json"  # return "folio_map"


def rdf2marc_to_s3(**kwargs) -> str:
    path = 'tmp/3eb5f480-60d6-4732-8daf-02d8d6f26eb7'
    os.makedirs(path, exist_ok=True)
    s3_hook = S3Hook(aws_conn_id='aws_lambda_connection')
    # logging.info(f"{s3_hook.list_prefixes(bucket_name='sinopia-marc-development')}")
    # s3_hook.download_file('marc/airflow/3eb5f480-60d6-4732-8daf-02d8d6f26eb7/record.mar', 'sinopia-marc-development', 'tmp/3eb5f480-60d6-4732-8daf-02d8d6f26eb7/record.mar')
    temp_file = s3_hook.download_file(
        key='marc/airflow/3eb5f480-60d6-4732-8daf-02d8d6f26eb7/record.mar',
        bucket_name='sinopia-marc-development',
        local_path=path
    )
    # ) as marc:
    with open(temp_file, "rb") as marc:
        marc_reader = MARCReader(marc)
        for record in marc_reader:
            if record is None:
                logging.info("Oops")
            else:
                s3_hook.load_string(record.as_json(), 'marc/airflow/3eb5f480-60d6-4732-8daf-02d8d6f26eb7/record.json', "sinopia-marc-development", replace=True)



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
    # TODO: This is currently a stub that only logs.
    listen_sns = SubscribeOperator(topic="stanford")

    urls = [
        "https://api.stage.sinopia.io/resource/3eb5f480-60d6-4732-8daf-02d8d6f26eb7",
    ]

    # Removing for the time being, not sure this is required yet or how it will fit.
    # branch_ils = BranchPythonOperator(task_id="ils", python_callable=choose_ils)

    run_rdf2marc = PythonOperator(
        task_id="symphony_json",
        python_callable=Rdf2Marc,
        op_kwargs={"instance_uri": urls[0]},
    )

    export_symphony_json = PythonOperator(
        task_id="symphony_json_to_s3",
        python_callable=rdf2marc_to_s3,
        op_kwargs={"urls": urls},
    )

    connect_symphony_cmd = """echo send POST to Symphony Web Services, returns CATKEY
    exit 0"""

    #  Send to Symphony Web API
    send_to_symphony = BashOperator(
        task_id="symphony_send", bash_command=connect_symphony_cmd
    )

    # Dummy Operator
    processed_sinopia = DummyOperator(
        task_id="processed_sinopia", dag=dag, trigger_rule="none_failed"
    )

    # Updates Sinopia URLS with HRID
    update_sinopia = UpdateIdentifier(urls=urls)

listen_sns >> run_rdf2marc
run_rdf2marc >> export_symphony_json >> send_to_symphony >> processed_sinopia
processed_sinopia >> update_sinopia
