import pytest  # noqa: F401
import rdflib

import ils_middleware.tasks.folio.mappings.bf_instance as bf_instance_map

uri = "https://api.stage.sinopia.io/resource/b0319047-acd0-4f30-bd8b-98e6c1bac6b0"


def test_date_of_publication(test_graph: rdflib.Graph):
    sparql = bf_instance_map.date_of_publication.format(bf_instance=uri)
    dates = [row[0] for row in test_graph.query(sparql)]

    assert str(dates[0]).startswith("2020")


def test_isbn(test_graph: rdflib.Graph):
    sparql = bf_instance_map.identifier.format(bf_instance=uri, bf_class="bf:Isbn")

    isbns = [row[0] for row in test_graph.query(sparql)]

    assert str(isbns[0]).startswith("9788869694110")
    assert str(isbns[1]).startswith("9788869694103")


def test_media_format(test_graph: rdflib.Graph):
    sparql = bf_instance_map.instance_format_category.format(bf_instance=uri)
    media_formats = [row[0] for row in test_graph.query(sparql)]

    assert str(media_formats[0]).startswith("http://id.loc.gov/vocabulary/mediaTypes/c")


def test_carrier_term(test_graph: rdflib.Graph):
    sparql = bf_instance_map.instance_format_term.format(bf_instance=uri)
    terms = [row[0] for row in test_graph.query(sparql)]

    assert str(terms[0]).startswith("http://id.loc.gov/vocabulary/carriers/cr")


def test_local_idenitifier(test_graph: rdflib.Graph):
    sparql = bf_instance_map.local_identifier.format(bf_instance=uri)
    local_idents = [row[0] for row in test_graph.query(sparql)]

    assert str(local_idents[0]).startswith("1272909598")


def test_mode_of_issuance(test_graph: rdflib.Graph):
    sparql = bf_instance_map.mode_of_issuance.format(bf_instance=uri)
    modes = [row[0] for row in test_graph.query(sparql)]

    assert str(modes[0]).startswith("http://id.loc.gov/vocabulary/issuance/mono")


def test_note(test_graph: rdflib.Graph):
    sparql = bf_instance_map.note.format(bf_instance=uri)
    notes = [row[0] for row in test_graph.query(sparql)]

    assert len(str(notes[0])) == 50
    assert len(str(notes[1])) == 90


def test_physical_description_dimensions(test_graph: rdflib.Graph):
    sparql = bf_instance_map.physical_description_dimensions.format(bf_instance=uri)
    dimensions = [row[0] for row in test_graph.query(sparql)]

    assert str(dimensions[0]).startswith("30 cm by 15 cm")


def test_physical_description_extent(test_graph: rdflib.Graph):
    sparql = bf_instance_map.physical_description_extent.format(bf_instance=uri)
    extents = [row[0] for row in test_graph.query(sparql)]

    assert str(extents[0]).startswith("1 online resource (128 pages)")


def test_place(test_graph: rdflib.Graph):
    sparql = bf_instance_map.place.format(bf_instance=uri)
    places = [row[0] for row in test_graph.query(sparql)]

    assert str(places[0]).startswith("Venice (Italy)")


def test_publisher(test_graph: rdflib.Graph):
    sparql = bf_instance_map.publisher.format(bf_instance=uri)
    publishers = [row[0] for row in test_graph.query(sparql)]

    assert str(publishers[0]).startswith("Edizioni Ca'Foscari")


def test_main_title(test_graph: rdflib.Graph):
    sparql = bf_instance_map.title.format(bf_instance=uri, bf_class="bf:Title")
    titles = [row for row in test_graph.query(sparql)]

    assert str(titles[0][0]).startswith("Scrivere di Islam")
    assert str(titles[0][1]).startswith("raccontere la diaspora")


def test_parallel_title(test_graph: rdflib.Graph):
    sparql = bf_instance_map.title.format(bf_instance=uri, bf_class="bf:ParallelTitle")
    titles = [row for row in test_graph.query(sparql)]

    assert str(titles[0][0]).startswith("Writing about Islam")
    assert str(titles[0][1]).startswith("narrating a diaspora")
