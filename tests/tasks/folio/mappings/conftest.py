import pathlib

import pytest
import rdflib

rdf_file = pathlib.Path(__file__).parent / "bf.ttl"

@pytest.fixture
def test_graph():
    graph = rdflib.Graph()
    for ns in [("bf", "http://id.loc.gov/ontologies/bibframe/"),
               ("bflc", "http://id.loc.gov/ontologies/bflc/"),
               ("sinopia", "http://sinopia.io/vocabulary/")]:
        graph.namespace_manager.bind(ns[0], ns[1])
    graph.parse(rdf_file, format='turtle')
    return graph