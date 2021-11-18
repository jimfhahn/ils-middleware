# placing this in the root allows the tests in the 'tests' directory to import the packages in the root, e.g. 'dags'
import pathlib
import sys

import pytest
import rdflib

root_dir = pathlib.Path(__file__).parent
rdf_file = root_dir / "tests" / "fixtures" / "bf.ttl"
helpers = root_dir / "tests" / "helpers"

sys.path.append(str(helpers))


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
