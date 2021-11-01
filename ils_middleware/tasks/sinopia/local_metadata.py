"""Adds Sinopia localAdminMetadata record."""

import json
import datetime
import logging
import uuid

import rdflib
import requests  # type: ignore

from airflow.models import Variable

logger = logging.getLogger(__name__)

BF = rdflib.Namespace("http://id.loc.gov/ontologies/bibframe/")
BFLC = rdflib.Namespace("http://id.loc.gov/ontologies/bflc/")
SINOPIA = rdflib.Namespace("http://sinopia.io/vocabulary/")


def _add_local_system_id(
    graph: rdflib.Graph, identifier: str, ils: str
) -> rdflib.BNode:
    """Adds Local System ID to the localSystemMetadata"""
    id_blank_node = rdflib.BNode()
    graph.add((id_blank_node, rdflib.RDF.type, BF.Local))
    graph.add((id_blank_node, rdflib.RDF.value, rdflib.Literal(identifier)))
    source_blank_node = rdflib.BNode()
    graph.add((id_blank_node, BF.source, source_blank_node))
    graph.add((source_blank_node, rdflib.RDF.type, BF.Source))
    graph.add((source_blank_node, rdflib.RDFS.label, rdflib.Literal(ils)))
    return id_blank_node


def create_admin_metadata(**kwargs) -> str:
    """Creates a Sinopia Local Admin Metadata graph and returns as JSON-LD."""
    admin_metadata_uri = kwargs.get("admin_metadata_uri", "")
    instance_uri = kwargs.get("instance_uri")
    ils_identifiers = kwargs.get("ils_identifiers", {})
    cataloger_id = kwargs.get("cataloger_id")

    graph = rdflib.Graph()
    local_admin_metadata = rdflib.URIRef(admin_metadata_uri)
    graph.add((local_admin_metadata, rdflib.RDF.type, BF.AdminMetadata))
    graph.add(
        (
            local_admin_metadata,
            SINOPIA.hasResourceTemplate,
            rdflib.Literal("pcc:sinopia:localAdminMetadata"),
        )
    )
    graph.add(
        (
            local_admin_metadata,
            SINOPIA.localAdminMetadataFor,
            rdflib.URIRef(instance_uri),
        )
    )
    if cataloger_id:
        graph.add(
            (local_admin_metadata, BFLC.catalogerId, rdflib.Literal(cataloger_id))
        )
    for ils, identifier in ils_identifiers.items():
        if identifier:
            ident_bnode = _add_local_system_id(graph, identifier, ils)
            graph.add((local_admin_metadata, BF.identifier, ident_bnode))
    export_date = datetime.datetime.utcnow().isoformat()
    graph.add((local_admin_metadata, SINOPIA.exportDate, rdflib.Literal(export_date)))
    return graph.serialize(format="json-ld")


def new_local_admin_metadata(*args, **kwargs) -> str:
    "Add Identifier to Sinopia localAdminMetadata."
    jwt = kwargs.get("jwt")
    group = kwargs.get("group")
    instance_uri = kwargs.get("instance_uri")
    user = Variable.get("sinopia_user")

    kwargs["cataloger_id"] = user
    sinopia_env = kwargs.get("sinopia_env", "dev")
    logger.debug(f"ILS Identifier {kwargs.get('ils_identifiers')}")

    sinopia_api_uri = Variable.get(f"{sinopia_env}_sinopia_api_uri")

    admin_metadata_uri = f"{sinopia_api_uri}/{uuid.uuid4()}"
    local_metadata_rdf = create_admin_metadata(
        **kwargs, admin_metadata_uri=admin_metadata_uri
    )

    local_metadata_rdf = json.loads(local_metadata_rdf)

    headers = {"Authorization": f"Bearer {jwt}", "Content-Type": "application/json"}

    sinopia_doc = {
        "data": local_metadata_rdf,
        "user": user,
        "group": group,
        "editGroups": [],
        "templateId": "pcc:sinopia:localAdminMetadata",
        "types": [],
        "bfAdminMetadataRefs": [],
        "bfItemRefs": [],
        "bfInstanceRefs": [
            instance_uri,
        ],
        "bfWorkRefs": [],
    }

    logger.debug(sinopia_doc)

    new_admin_result = requests.post(
        admin_metadata_uri,
        json=sinopia_doc,
        headers=headers,
    )

    if new_admin_result.status_code > 399:
        msg = f"Failed to add localAdminMetadata, {new_admin_result.status_code}\n{new_admin_result.text}"
        logger.error(msg)
        raise Exception(msg)

    logger.debug(f"Results of new_admin_result {new_admin_result.text}")

    return admin_metadata_uri