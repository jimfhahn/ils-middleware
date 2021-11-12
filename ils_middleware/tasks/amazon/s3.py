import json
import logging
from urllib.parse import urlparse
from os import path
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pymarc import MARCReader

logger = logging.getLogger(__name__)


def get_from_s3(**kwargs) -> str:
    s3_hook = S3Hook(aws_conn_id="aws_lambda_connection")
    task_instance = kwargs.get("task_instance")
    resources = task_instance.xcom_pull(key="resources", task_ids=["sqs-message-parse"])

    for instance_uri in resources:
        instance_path = urlparse(instance_uri).path
        instance_id = path.split(instance_path)[-1]

        temp_file = s3_hook.download_file(
            key=f"marc/airflow/{instance_id}/record.mar",
            bucket_name="sinopia-marc-development",
        )
        task_instance.xcom_push(key=instance_uri, value=temp_file)


def send_to_s3(**kwargs) -> dict:
    s3_hook = S3Hook(aws_conn_id="aws_lambda_connection")
    instances = kwargs.get("instances")
    for instance in instances:
        marc_record = marc_record_from_temp_file(instance)

        s3_hook.load_string(
            marc_record.as_json(),
            f"marc/airflow/{instance['id']}/record.json",
            "sinopia-marc-development",
            replace=True,
        )

    return marc_record.as_json()


def marc_record_from_temp_file(instance):
    instance_id = instance["id"]
    temp_file = instance["temp_file"]
    if os.path.exists(temp_file) and os.path.getsize(temp_file) > 0:
        with open(temp_file, "rb") as marc:
            return next(MARCReader(marc))
    else:
        logger.error(f"MARC data for {instance_id} missing or empty.")
        raise Exception()
