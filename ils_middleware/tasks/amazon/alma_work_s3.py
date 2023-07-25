import logging
from urllib.parse import urlparse
import os
from os import path
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pymarc import MARCReader
from rdflib import Graph

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


def send_work_to_alma_s3(**kwargs):
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
                work_field = record.get_fields("758")
                work_uri = work_field[0].get_subfields("0")[0]
                logger.info(f"Work URI: {work_uri}")
            g = Graph()
            g.parse(work_uri)
            # not very DRY?
            g.bind("xmlns", "http://www.w3.org/2000/xmlns/")
            g.bind("xsd", "http://www.w3.org/2001/XMLSchema#")
            g.bind("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
            g.bind("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
            g.bind("bf", "http://id.loc.gov/ontologies/bibframe/")
            g.bind("bflc", "http://id.loc.gov/ontologies/bflc/")
            g.bind("madsrdf", "http://www.loc.gov/mads/rdf/v1#")
            g.bind("sinopia", "http://sinopia.io/vocabulary/")
            g.bind("sinopiabf", "http://sinopia.io/vocabulary/bf/")
            g.bind("rdau", "http://rdaregistry.info/Elements/u/")
            g.bind("owl", "http://www.w3.org/2002/07/owl#")
            g.bind("skos", "http://www.w3.org/2004/02/skos/core#")
            g.bind("dcterms", "http://purl.org/dc/terms/")
            g.bind("cc", "http://creativecommons.org/ns#")
            g.bind("foaf", "http://xmlns.com/foaf/0.1/")
            # serialize to xml
            bfwork_alma_xml = g.serialize(format="pretty-xml")
            s3_hook.load_bytes(
                bfwork_alma_xml,
                f"/alma/{instance_id}/bfwork_alma.xml",
                Variable.get("marc_s3_bucket"),
                replace=True,
            )
        task_instance.xcom_push(
            key=instance_uri, value=f"/alma/{instance_id}/bfwork_alma.xml"
        )
        logger.info(f"Saved BFWork description for {instance_id} to alma.")


def marc_record_from_temp_file(instance_id, temp_file):
    if os.path.exists(temp_file) and os.path.getsize(temp_file) > 0:
        with open(temp_file, "rb") as marc:
            return next(MARCReader(marc))
    else:
        logger.error(f"MARC data for {instance_id} missing or empty.")
