import logging
import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pymarc import MARCReader
from urllib.parse import urlparse


def get_from_s3(**kwargs) -> list:
    urls = kwargs.get('urls')
    s3_hook = S3Hook(aws_conn_id='aws_lambda_connection')
    downloaded_files = []

    for url in urls:
        instance_path = urlparse(url).path
        instance_id = os.path.split(instance_path)[-1]

        temp_file = s3_hook.download_file(
            key=f"marc/airflow/{instance_id}/record.mar",
            bucket_name='sinopia-marc-development'
        )
        downloaded_files.append({'id': instance_id, 'temp_file': temp_file})

    return downloaded_files


def send_to_s3(**kwargs) -> str:
    s3_hook = S3Hook(aws_conn_id='aws_lambda_connection')

    task_instance = kwargs['task_instance']
    instances = task_instance.xcom_pull(task_ids='process_symphony.download_symphony_marc')

    for instance in instances:
        instance_id = instance['id']
        temp_file = instance['temp_file']

        with open(temp_file, "rb") as marc:
            marc_reader = MARCReader(marc)
            for record in marc_reader:
                if record is None:
                    logging.info("Oops")
                else:
                    s3_hook.load_string(
                        record.as_json(),
                        f"marc/airflow/{instance_id}/record.json",
                        "sinopia-marc-development",
                        replace=True)
