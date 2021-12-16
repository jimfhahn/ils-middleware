import logging
from urllib.parse import urlparse
import os
from os import path
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from lxml import etree
from pymarc import XMLWriter
from pymarc import MARCReader

logger = logging.getLogger(__name__)


def get_from_alma_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id="aws_lambda_connection")
    task_instance = kwargs.get("task_instance")
    resources = task_instance.xcom_pull(key="resources", task_ids="sqs-message-parse")

    for instance_uri in resources:
        instance_path = urlparse(instance_uri).path
        instance_id = path.split(instance_path)[-1]

        temp_file = s3_hook.download_file(
            key=f"marc/airflow/{instance_id}/record.mar",
            bucket_name=Variable.get("marc_s3_bucket"),
        )
        task_instance.xcom_push(key=instance_uri, value=temp_file)


def send_to_alma_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id="aws_lambda_connection")
    task_instance = kwargs.get("task_instance")
    resources = task_instance.xcom_pull(key="resources", task_ids="sqs-message-parse")

    for instance_uri in resources:
        instance_path = urlparse(instance_uri).path
        instance_id = path.split(instance_path)[-1]

        temp_file = task_instance.xcom_pull(
            key=instance_uri, task_ids="process_alma.download_marc"
        )
        marc_file = marc_record_from_temp_file(instance_id, temp_file)
        with open(temp_file, "rb") as marc_file:
            reader = MARCReader(marc_file)
            for record in reader:
                writer = XMLWriter(open("xml_file.xml", "wb"))
                writer.write(record)
                writer.close()
                tree = etree.parse("xml_file.xml")
                root = tree.getroot()
                newroot = etree.Element(
                    "bib"
                )  # insert the <bib> root element required by alma
                newroot.insert(0, root)
                alma_xml = etree.tostring(
                    newroot, xml_declaration=True, encoding="utf-8"
                )
                f = open("alma.xml", "wb")
                f.write(alma_xml)
                f.close()
        s3_hook.load_bytes(
            alma_xml,
            bucket_name=Variable.get("marc_s3_bucket"),
            replace=True,
            key=f"marc/airflow/{instance_id}/alma.xml",
        )


def marc_record_from_temp_file(instance_id, temp_file):
    if os.path.exists(temp_file) and os.path.getsize(temp_file) > 0:
        with open(temp_file, "rb") as marc:
            return next(MARCReader(marc))
    else:
        logger.error(f"MARC data for {instance_id} missing or empty.")
