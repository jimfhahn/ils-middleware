from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from ils_middleware.tasks.amazon.s3 import get_from_s3, send_to_s3
from ils_middleware.tasks.amazon.sqs import SubscribeOperator, parse_messages
from ils_middleware.tasks.sinopia.sinopia import UpdateIdentifier
from ils_middleware.tasks.sinopia.email import email_for_success
from ils_middleware.tasks.sinopia.rdf2marc import Rdf2Marc
from ils_middleware.tasks.symphony.login import SymphonyLogin
from ils_middleware.tasks.symphony.new import NewMARCtoSymphony
from ils_middleware.tasks.symphony.mod_json import to_symphony_json


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

    with TaskGroup(group_id="process_symphony") as symphony_task_group:
        run_rdf2marc = PythonOperator(
            task_id="rdf2marc",
            python_callable=Rdf2Marc,
            op_kwargs={
                "instance_uri": "{{ task_instance.xcom_pull(task_ids='sqs-message-parse', key='resource_uri') }}",
                "rdf2marc_lambda": "sinopia-rdf2marc-development",
                "s3_bucket": "sinopia-marc-development",
            },
        )

        download_marc = PythonOperator(
            task_id="download_marc",
            python_callable=get_from_s3,
            op_kwargs={
                "instance_id": "{{ task_instance.xcom_pull(task_ids='process_symphony.rdf2marc', key='return_value') }}"
            },
        )

        export_marc_json = PythonOperator(
            task_id="marc_json_to_s3",
            python_callable=send_to_s3,
            op_kwargs={
                "instance": "{{ task_instance.xcom_pull(task_ids='process_symphony.download_marc', key='return_value') }}"
            },
        )

        convert_to_symphony_json = PythonOperator(
            task_id="convert_to_symphony_json",
            python_callable=to_symphony_json,
            op_kwargs={
                "marc_json": "{{ task_instance.xcom_pull(task_ids='process_symphony.marc_json_to_s3', key='return_value') }}"
            },
        )

        # Symphony Dev Server Settings
        library_key = "GREEN"
        home_location = "STACKS"
        symphony_app_id = "SINOPIA_DEV"
        symphony_client_id = "SymWSStaffClient"
        symphony_conn_id = "stanford_symphony_dev"
        # This could be mapped from the Instance RDF template
        symphony_item_type = "STKS-MONO"

        symphony_login = PythonOperator(
            task_id="symphony-login",
            python_callable=SymphonyLogin,
            op_kwargs={
                "app_id": symphony_app_id,
                "client_id": symphony_client_id,
                "conn_id": symphony_conn_id,
                "login": Variable.get("stanford_symphony_dev_login"),
                "password": Variable.get("stanford_symphony_dev_password"),
            },
        )

        symphony_add_record = PythonOperator(
            task_id="post_new_symphony",
            python_callable=NewMARCtoSymphony,
            op_kwargs={
                "app_id": symphony_app_id,
                "client_id": symphony_client_id,
                "conn_id": symphony_conn_id,
                "home_location": home_location,
                "item_type": symphony_item_type,
                "library_key": library_key,
                "marc_json": """{{ task_instance.xcom_pull(key='return_value',
                                      task_ids='process_symphony.convert_to_symphony_json')}}""",
                "token": "{{ task_instance.xcom_pull(key='return_value', task_ids='process_symphony.symphony-login')}}",
            },
        )

        (
            run_rdf2marc
            >> download_marc
            >> export_marc_json
            >> convert_to_symphony_json
            >> [symphony_login >> symphony_add_record]
        )

    with TaskGroup(group_id="process_folio") as folio_task_group:
        download_folio_marc = DummyOperator(task_id="download_folio_marc", dag=dag)

        export_folio_json = DummyOperator(task_id="folio_json_to_s3", dag=dag)

        send_to_folio = DummyOperator(task_id="folio_send", dag=dag)

        download_folio_marc >> export_folio_json >> send_to_folio

    # Dummy Operator
    processed_sinopia = DummyOperator(
        task_id="processed_sinopia", dag=dag, trigger_rule="none_failed"
    )

    # Updates Sinopia URLS with ILS Identifier
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


(
    listen_sns
    >> process_message
    >> [symphony_task_group, folio_task_group]
    >> processed_sinopia
)
processed_sinopia >> update_sinopia >> notify_sinopia_updated
