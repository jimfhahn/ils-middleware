from datetime import datetime, timedelta

from ils_middleware.tasks.amazon.s3 import get_from_s3, send_to_s3
from ils_middleware.tasks.amazon.sqs import SubscribeOperator, parse_messages
from ils_middleware.tasks.sinopia.sinopia import UpdateIdentifier
from ils_middleware.tasks.sinopia.email import email_for_success
from ils_middleware.tasks.sinopia.rdf2marc import Rdf2Marc


from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
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
    "provide_context": True,
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
    # By default, SubscribeOperator will make the message available via XCom: "Get messages from an SQS queue and then
    # deletes the message from the SQS queue. If deletion of messages fails an AirflowException is thrown otherwise, the
    # message is pushed through XCom with the key 'messages'."
    # https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/sensors/sqs/index.html
    listen_sns = SubscribeOperator(topic="stanford")

    process_message = PythonOperator(
        task_id="sqs-message-parse",
        python_callable=parse_messages,
    )

    run_rdf2marc = PythonOperator(
        task_id="symphony_json",
        python_callable=Rdf2Marc,
        op_kwargs={
            "instance_uri": "{{ task_instance.xcom_pull(task_ids='sqs-message-parse', key='resource_uri') }}"
        },
    )

    with TaskGroup(group_id="process_symphony") as symphony_task_group:
        download_symphony_marc = PythonOperator(
            task_id="download_symphony_marc",
            python_callable=get_from_s3,
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
    update_sinopia = PythonOperator(
        task_id="sinopia-id-update",
        python_callable=UpdateIdentifier,
    )

    notify_sinopia_updated = PythonOperator(
        task_id="sinopia_update_success_notification",
        dag=dag,
        trigger_rule="none_failed",
        python_callable=email_for_success,
    )

listen_sns >> process_message >> run_rdf2marc
run_rdf2marc >> [symphony_task_group, folio_task_group] >> processed_sinopia
processed_sinopia >> update_sinopia >> notify_sinopia_updated
