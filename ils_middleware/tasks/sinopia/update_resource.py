"""Updates Resource with new local Admin Metadata"""
import json
import logging

import rdflib
import requests  # type: ignore

logger = logging.getLogger(__name__)

BF = rdflib.Namespace("http://id.loc.gov/ontologies/bibframe/")


def _get_update_rdf(resource_uri, metadata_uri, raw_json_ld: str) -> str:
    resource_uri_node = rdflib.URIRef(resource_uri)
    metadata_uri_node = rdflib.URIRef(metadata_uri)

    graph = rdflib.Graph()

    graph.parse(data=raw_json_ld, format="json-ld")
    graph.add((resource_uri_node, BF.adminMetadata, metadata_uri_node))
    return graph.serialize(format="json-ld")


def update_resource_new_metadata(*args, **kwargs) -> str:
    """Updates Resource RDF with new local AdminMetadata URI"""
    jwt = kwargs.get("jwt")
    task_instance = kwargs["task_instance"]
    resources = task_instance.xcom_pull(key="resources", task_ids=["sqs-message-parse"])

    updated_resources = []
    update_failed = []
    resource_not_found = []

    for resource in resources:
        resource_uri = resource.get("resource_uri")
        metadata_uri = resource.get("metadata_uri")

        result = requests.get(resource_uri)

        if result.status_code > 399:
            msg = f"{resource_uri} retrieval error {result.status_code}\n{result.text}"
            logger.error(msg)
            resource_not_found.append(resource_uri)
            continue

        sinopia_doc = result.json()
        updated_json_ld = _get_update_rdf(
            resource_uri, metadata_uri, json.dumps(sinopia_doc.get("data"))
        )

        headers = {"Authorization": f"Bearer {jwt}", "Content-Type": "application/json"}

        sinopia_doc["data"] = json.loads(updated_json_ld)
        sinopia_doc["bfAdminMetadataRefs"].append(metadata_uri)

        update_result = requests.put(resource_uri, json=sinopia_doc, headers=headers)

        if update_result.status_code > 399:
            msg = f"Failed to update {resource_uri}, status code {update_result.status_code}\n{update_result.text}"
            logger.error(msg)
            update_failed.append(resource_uri)
            continue

        updated_resources.append(resource_uri)

    task_instance.xcom_push(key="updated_resources", value=updated_resources)
    task_instance.xcom_push(key="update_failed", value=update_failed)
    task_instance.xcom_push(key="resource_not_found", value=resource_not_found)
