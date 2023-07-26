import logging
from urllib.parse import urlparse
import os
from os import path
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pymarc import MARCReader
from rdflib import Graph
import lxml.etree as ET

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
            h.bind("xmlns", "http://www.w3.org/2000/xmlns/")
            h.bind("xsd", "http://www.w3.org/2001/XMLSchema#")
            h.bind("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
            h.bind("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
            h.bind("bf", "http://id.loc.gov/ontologies/bibframe/")
            h.bind("bflc", "http://id.loc.gov/ontologies/bflc/")
            h.bind("madsrdf", "http://www.loc.gov/mads/rdf/v1#")
            h.bind("sinopia", "http://sinopia.io/vocabulary/")
            h.bind("sinopiabf", "http://sinopia.io/vocabulary/bf/")
            h.bind("rdau", "http://rdaregistry.info/Elements/u/")
            h.bind("owl", "http://www.w3.org/2002/07/owl#")
            h.bind("skos", "http://www.w3.org/2004/02/skos/core#")
            h.bind("dcterms", "http://purl.org/dc/terms/")
            h.bind("cc", "http://creativecommons.org/ns#")
            h.bind("foaf", "http://xmlns.com/foaf/0.1/")
            bfinstance_alma_xml = h.serialize(format="pretty-xml", encoding="utf-8")
            tree = ET.fromstring(bfinstance_alma_xml)
            # apply xslt to normalize instance
            xslt = ET.parse("tests/fixtures/xslt/normalize-instance.xsl")
            transform = ET.XSLT(xslt)
            bfinstance_alma_xml = transform(tree)
            bfinstance_alma_xml = ET.tostring(bfinstance_alma_xml, pretty_print=True)
            logger.info(f"Normalized BFInstance description for {instance_id}.")
            # post to s3 as bytes
            s3_hook.load_bytes(
                bfinstance_alma_xml,
                f"alma/airflow/{instance_id}/bfinstance_alma.xml",
                Variable.get("marc_s3_bucket"),
                replace=True,
            )

        task_instance.xcom_push(
            key=instance_uri, value=f"/alma/{instance_id}/bfinstance_alma.xml"
        )

        logger.info(f"Saved BFInstance description for {instance_id} to alma.")


def marc_record_from_temp_file(instance_id, temp_file):
    if os.path.exists(temp_file) and os.path.getsize(temp_file) > 0:
        with open(temp_file, "rb") as marc:
            return next(MARCReader(marc))
    else:
        logger.error(f"MARC data for {instance_id} missing or empty.")
