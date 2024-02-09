import logging
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from rdflib import Graph, URIRef, Namespace
from rdflib.namespace import RDF, RDFS
from lxml import etree as ET
from ils_middleware.tasks.amazon.alma_ns import alma_namespaces

logger = logging.getLogger(__name__)


def send_work_to_alma_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id="aws_lambda_connection")
    task_instance = kwargs.get("task_instance")
    resources = task_instance.xcom_pull(key="resources", task_ids="sqs-message-parse")

    for instance_uri in resources:
        instance_uri = URIRef(instance_uri)
        work_uri = None
        instance_graph = Graph()
        work_graph = Graph()
        instance_graph.parse(instance_uri)
        # Define the bf and bflc namespaces
        bf = Namespace("http://id.loc.gov/ontologies/bibframe/")
        for prefix, url in alma_namespaces:
            work_graph.bind(prefix, url)
        work_uri = instance_graph.value(
            subject=URIRef(instance_uri), predicate=bf.instanceOf
        )
        if work_uri is None:
            logger.info(f"Instance {instance_uri} has no work.")
            continue
        work_uri = URIRef(work_uri)
        # Explicitly state that work_uri is of type bf:Work
        work_graph.add((work_uri, RDF.type, bf.Work))
        # parse the work graph
        work_graph.parse(work_uri)
        # add the instance to the work graph
        work_graph.add((work_uri, bf.hasInstance, URIRef(instance_uri)))
        # Find all bflc:PrimaryContribution instances and their associated bf:agent URIs
        for s, p, o in work_graph.triples((None, bf.agent, None)):
            # s is the subject of the triple, which should be a bflc:PrimaryContribution
            primary_contribution = s
            # o is the object of the triple, which should be the bf:agent URI
            agent_uri = o
            # get the label of the agent
            agent_label = work_graph.value(subject=agent_uri, predicate=RDFS.label)
            # add it to the primary contribution
            work_graph.add((primary_contribution, RDFS.label, agent_label))

        # serialize the work graph
        bfwork_alma_xml = work_graph.serialize(format="pretty-xml", encoding="utf-8")
        tree = ET.fromstring(bfwork_alma_xml)

        # apply xslt to normalize work
        xslt = ET.parse("ils_middleware/tasks/amazon/xslt/normalize-work.xsl")
        transform = ET.XSLT(xslt)
        bfwork_alma_xml = transform(tree)
        bfwork_alma_xml = ET.tostring(
            bfwork_alma_xml, pretty_print=True, encoding="utf-8"
        )
        logger.info(f"Normalized BFWork description for {work_uri}.")
        # post to s3 as bytes
        s3_hook.load_bytes(
            bfwork_alma_xml,
            f"alma/{instance_uri}/bfwork_alma.xml",
            Variable.get("marc_s3_bucket"),
            replace=True,
        )
        # save to xcom
        task_instance.xcom_push(key=instance_uri, value=bfwork_alma_xml.decode("utf-8"))
        logger.info(f"Saved BFWork description(s) for {work_uri} to alma.")
        logger.info(f"bf_work_alma_xml: {bfwork_alma_xml.decode('utf-8')}")
