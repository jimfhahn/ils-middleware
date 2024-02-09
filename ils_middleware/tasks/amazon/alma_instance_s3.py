import logging
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from rdflib import Graph, URIRef, Namespace
from rdflib.namespace import RDF
from lxml import etree as ET
from ils_middleware.tasks.amazon.alma_ns import alma_namespaces

logger = logging.getLogger(__name__)


def send_instance_to_alma_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id="aws_lambda_connection")
    task_instance = kwargs.get("task_instance")
    resources = task_instance.xcom_pull(key="resources", task_ids="sqs-message-parse")

    for instance_uri in resources:
        instance_uri = URIRef(instance_uri)
        work_uri = None
        instance_graph = Graph()
        work_graph = Graph()
        instance_graph.parse(instance_uri)
        # Define the bf namespace
        bf = Namespace("http://id.loc.gov/ontologies/bibframe/")
        for prefix, url in alma_namespaces:
            instance_graph.bind(prefix, url)
        work_uri = instance_graph.value(
            subject=URIRef(instance_uri), predicate=bf.instanceOf
        )
        if work_uri is None:
            logger.info(f"Instance {instance_uri} has no work.")
            continue
        work_uri = URIRef(work_uri)
        # Explicitly state that work_uri is of type bf:Work
        work_graph.add((work_uri, RDF.type, bf.Work))
        # add the work to the instance graph
        instance_graph.add((instance_uri, bf.instanceOf, URIRef(work_uri)))
        # serialize the instance graph
        instance_alma_xml = instance_graph.serialize(
            format="pretty-xml", encoding="utf-8"
        )
        logger.info(instance_alma_xml)
        tree = ET.fromstring(instance_alma_xml)
        logger.info(instance_alma_xml)
        # apply xslt to normalize instance description
        xslt = ET.parse("ils_middleware/tasks/amazon/xslt/normalize-instance.xsl")
        transform = ET.XSLT(xslt)
        instance_alma_xml = transform(tree)
        instance_alma_xml = ET.tostring(
            instance_alma_xml, pretty_print=True, encoding="utf-8"
        )
        logger.info(f"Normalized BFInstance description for {instance_uri}.")
        # post to s3 as bytes
        s3_hook.load_bytes(
            instance_alma_xml,
            f"alma/{instance_uri}/bfinstance_alma.xml",
            Variable.get("marc_s3_bucket"),
            replace=True,
        )
        # save to xcom
        task_instance.xcom_push(
            key=instance_uri, value=instance_alma_xml.decode("utf-8")
        )
        logger.info(f"Saved BFInstance description for {instance_uri} to alma.")
        logger.info(f"bf_instance_alma_xml: {instance_alma_xml.decode('utf-8')}")
