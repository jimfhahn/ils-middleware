import json
import logging
import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pymarc import MARCReader

logger = logging.getLogger(__name__)


def get_from_s3(**kwargs) -> str:
    instance_id = kwargs.get("instance_id")
    s3_hook = S3Hook(aws_conn_id="aws_lambda_connection")

    temp_file = s3_hook.download_file(
        key=f"marc/airflow/{instance_id}/record.mar",
        bucket_name="sinopia-marc-development",
    )

    return json.dumps({"id": instance_id, "temp_file": temp_file})


def send_to_s3(**kwargs) -> dict:
    s3_hook = S3Hook(aws_conn_id="aws_lambda_connection")
    instance = kwargs.get("instance")
    if isinstance(instance, str):
        instance = json.loads(instance)
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
