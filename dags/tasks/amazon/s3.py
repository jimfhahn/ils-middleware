import logging
import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pymarc import MARCReader
from urllib.parse import urlparse


def get_from_s3(**kwargs) -> list:
    url = kwargs.get("url")
    s3_hook = S3Hook(aws_conn_id="aws_lambda_connection")

    instance_path = urlparse(url).path
    instance_id = os.path.split(instance_path)[-1]

    temp_file = s3_hook.download_file(
        key=f"marc/airflow/{instance_id}/record.mar",
        bucket_name="sinopia-marc-development",
    )

    return {"id": instance_id, "temp_file": temp_file}


def send_to_s3(**kwargs) -> str:
    s3_hook = S3Hook(aws_conn_id="aws_lambda_connection")

    instance = get_temp_instances(kwargs["task_instance"])

    marc_reader = read_temp_file(instance)

    for record in marc_reader:
        s3_hook.load_string(
            marc_json_for(record),
            f"marc/airflow/{instance['id']}/record.json",
            "sinopia-marc-development",
            replace=True,
        )


def get_temp_instances(task_instance):
    return task_instance.xcom_pull(task_ids="process_symphony.download_symphony_marc")


def read_temp_file(instance):
    instance_id = instance["id"]
    temp_file = instance["temp_file"]
    if os.path.exists(temp_file) and os.path.getsize(temp_file) > 0:
        return marc_reader_for(temp_file)
    else:
        logging.error(f"MARC data for {instance_id} missing or empty.")
        raise Exception()


def marc_reader_for(temp_file):
    with open(temp_file, "rb") as marc:
        return MARCReader(marc)


def marc_json_for(marc_record):
    return marc_record.as_json()
