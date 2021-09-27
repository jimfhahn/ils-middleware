import logging

from datetime import datetime, timedelta

from tasks.amazon.s3 import get_from_s3, send_to_s3
from tasks.amazon.sqs import SubscribeOperator
from tasks.sinopia.sinopia import UpdateIdentifier
from tasks.sinopia.rdf2marc import Rdf2Marc


from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator


def choose_ils(**kwargs) -> str:
    import random

    ils_choice = random.random()
    logging.info(f"Random ILS Choice {ils_choice}")
    if ils_choice <= 0.50:
        return "symphony_json"
    return "symphony_json"  # return "folio_map"


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
    # Monitors SQS for Stanford topic
    listen_sns = SubscribeOperator(topic="stanford")

    urls = [
        "https://api.stage.sinopia.io/resource/3eb5f480-60d6-4732-8daf-02d8d6f26eb7",
    ]

    run_rdf2marc = PythonOperator(
        task_id="symphony_json",
        python_callable=Rdf2Marc,
    )

    with TaskGroup(group_id="process_symphony") as symphony_task_group:
        download_symphony_marc = PythonOperator(
            task_id="download_symphony_marc",
            python_callable=get_from_s3,
            op_kwargs={"urls": urls},
        )

        export_symphony_json = PythonOperator(
            task_id="symphony_json_to_s3",
            python_callable=send_to_s3,
        )

        connect_symphony_cmd = """echo send POST to Symphony Web Services, returns CATKEY
        exit 0"""

        #  Send to Symphony Web API
        send_to_symphony = BashOperator(
            task_id="symphony_send", bash_command=connect_symphony_cmd
        )

        download_symphony_marc >> export_symphony_json >> send_to_symphony

    with TaskGroup(group_id="process_folio") as folio_task_group:
        download_folio_marc = DummyOperator(task_id="download_folio_marc", dag=dag)

        export_folio_json = DummyOperator(task_id="folio_json_to_s3", dag=dag)

        send_to_folio = DummyOperator(task_id="folio_send", dag=dag)

        download_folio_marc >> export_folio_json >> send_to_folio

    # Dummy Operator
    processed_sinopia = DummyOperator(
        task_id="processed_sinopia", dag=dag, trigger_rule="none_failed"
    )

    # Updates Sinopia URLS with HRID
    update_sinopia = UpdateIdentifier(urls=urls)

listen_sns >> run_rdf2marc
run_rdf2marc >> [symphony_task_group, folio_task_group] >> processed_sinopia
processed_sinopia >> update_sinopia
