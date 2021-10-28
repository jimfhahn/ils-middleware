"""Retrieves related AdminMetadata resource info for downstream tasks."""
import datetime
import logging

import rdflib
import requests  # type: ignore

from typing import Optional

logger = logging.getLogger(__name__)


def query_for_ils_info(graph_jsonld: str, uri: str) -> dict:
    graph = rdflib.Graph()
    graph.parse(data=graph_jsonld, format="json-ld")
    output = {}
    for row in graph.query(
        f"""PREFIX sinopia: <http://sinopia.io/vocabulary/>
    PREFIX bf: <http://id.loc.gov/ontologies/bibframe/>

    SELECT ?export_date ?identifier ?ils
    WHERE {{
        <{uri}> sinopia:exportDate ?export_date .
        <{uri}> bf:identifier ?ident_bnode .
        ?ident_bnode rdf:value ?identifier .
        ?ident_bnode bf:source ?source .
        ?source rdfs:label ?ils .
    }}
    """
    ):
        output["export_date"] = datetime.datetime.fromisoformat(str(row[0]))
        output[str(row[2])] = str(row[1])  # type: ignore
    return output


def get_retrieve_metadata(uri: str) -> Optional[dict]:
    """Retrieves AdminMetadata resource and extracts any ILS identifiers"""
    metadata_result = requests.get(uri)
    if metadata_result.status_code > 399:
        msg = f"{uri} retrieval failed {metadata_result.status_code}\n{metadata_result.text}"
        logging.error(msg)
        raise Exception(msg)

    resource = metadata_result.json()

    # Ignore and return if not using the pcc:sinopia:localAdminMetadata template
    if not resource.get("templateId").startswith("pcc:sinopia:localAdminMetadata"):
        return None
    return query_for_ils_info(resource.get("data"), uri)


def check_return_refs(resource_refs_uri: str) -> list:
    resource_ref_results = requests.get(resource_refs_uri)
    if resource_ref_results.status_code > 399:
        msg = f"{resource_refs_uri} retrieval failed {resource_ref_results.status_code}\n{resource_ref_results.text}"
        logging.error(msg)
        raise Exception(msg)
    return resource_ref_results.json().get("bfAdminMetadataAllRefs", [])


def retrieve_metadata(bf_admin_metadata_all: list) -> list:
    ils_info = []
    for metadata_uri in bf_admin_metadata_all:
        metadata = get_retrieve_metadata(metadata_uri)
        if metadata:
            ils_info.append(metadata)
    return ils_info


def existing_metadata_check(*args, **kwargs) -> Optional[str]:
    """Queries Sinopia API for related resources of an instance."""
    task_instance = kwargs["task_instance"]
    resource_uri = kwargs.get("resource_uri")
    ils_tasks = kwargs.get("ils_tasks", {})

    bf_admin_metadata_all = check_return_refs(f"{resource_uri}/references")

    if len(bf_admin_metadata_all) < 1:
        return ils_tasks.get("new")

    ils_info = retrieve_metadata(bf_admin_metadata_all)

    if len(ils_info) < 1:
        return ils_tasks.get("new")

    # Sort retrieved ILS by date
    ils_info = sorted(ils_info, key=lambda x: x["export_date"], reverse=True)

    # Add only the latest ILS information to XCOM
    for key, value in ils_info[0].items():
        if key.startswith("export_date"):
            continue
        task_instance.xcom_push(key=key, value=value)

    return ils_tasks.get("overlay")
