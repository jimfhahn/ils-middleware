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

    run_rdf2marc = PythonOperator(
        task_id="symphony_json",
        python_callable=Rdf2Marc,
    )

    sinopia_to_folio_records = PythonOperator(
        task_id="folio_map",
        python_callable=map_to_folio,
        op_kwargs={"url": "http://example-instance.sinopia.io/"},
    )

    # Dummy Operator
    processed_sinopia = DummyOperator(
        task_id="processed_sinopia", dag=dag, trigger_rule="none_failed"
    )

    # Updates Sinopia URLS with HRID
    update_sinopia = UpdateIdentifier()

listen_sns >> run_rdf2marc
run_rdf2marc >> [symphony_task_group, folio_task_group] >> processed_sinopia
processed_sinopia >> update_sinopia
