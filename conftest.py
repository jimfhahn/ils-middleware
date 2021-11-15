# placing this in the root allows the tests in the 'tests' directory to import the packages in the root, e.g. 'dags'
import pathlib

import pytest
import rdflib

rdf_file = pathlib.Path(__file__).parent / "tests" / "fixtures" / "bf.ttl"


@pytest.fixture
def test_graph():
    graph = rdflib.Graph()
    for ns in [
        ("bf", "http://id.loc.gov/ontologies/bibframe/"),
        ("bflc", "http://id.loc.gov/ontologies/bflc/"),
        ("sinopia", "http://sinopia.io/vocabulary/"),
    ]:
        graph.namespace_manager.bind(ns[0], ns[1])
    graph.parse(rdf_file, format="turtle")
    return graph
