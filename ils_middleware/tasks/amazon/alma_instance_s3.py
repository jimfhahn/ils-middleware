import logging
from urllib.parse import urlparse
import os
from os import path
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pymarc import MARCReader
from rdflib import Graph
import lxml.etree as ET
from ils_middleware.tasks.amazon.alma_ns import alma_namespaces

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


def send_instance_to_alma_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id="aws_lambda_connection")
    task_instance = kwargs.get("task_instance")
    resources = task_instance.xcom_pull(key="resources", task_ids="sqs-message-parse")

    for instance_uri in resources:
        instance_path = urlparse(instance_uri).path
        instance_id = path.split(instance_path)[-1]

        temp_file = task_instance.xcom_pull(
            key=instance_uri, task_ids="process_alma.download_marc"
        )
        marc_record_from_temp_file(instance_id, temp_file)
        with open(temp_file, "rb") as marc_file:
            reader = MARCReader(marc_file)
            for record in reader:
                instance_field = record.get_fields("884")
                instance_uri = instance_field[0].get_subfields("k")[0]
                logger.info(f"Instance URI: {instance_uri}")
            h = Graph()
            h.parse(instance_uri)
            # declare namespaces
            for prefix, url in alma_namespaces:
                h.bind(prefix, url)
            bfinstance_alma_xml = h.serialize(format="pretty-xml", encoding="utf-8")
            tree = ET.fromstring(bfinstance_alma_xml)
            # apply xslt to normalize instance
            xslt = ET.parse("ils_middleware/tasks/amazon/xslt/normalize-instance.xsl")
            transform = ET.XSLT(xslt)
            bfinstance_alma_xml = transform(tree)
            bfinstance_alma_xml = ET.tostring(
                bfinstance_alma_xml, pretty_print=True, encoding="utf-8"
            )
            logger.info(f"Normalized BFInstance description for {instance_id}.")
            # post to s3 as bytes
            s3_hook.load_bytes(
                bfinstance_alma_xml,
                f"/alma/{instance_id}/bfinstance_alma.xml",
                Variable.get("marc_s3_bucket"),
                replace=True,
            )

        task_instance.xcom_push(
            key=instance_uri, value=bfinstance_alma_xml.decode("utf-8")
        )
        logger.info(f"Saved BFInstance description for {instance_id} to alma.")


def marc_record_from_temp_file(instance_id, temp_file):
    if os.path.exists(temp_file) and os.path.getsize(temp_file) > 0:
        with open(temp_file, "rb") as marc:
            return next(MARCReader(marc))
    else:
        logger.error(f"MARC data for {instance_id} missing or empty.")
