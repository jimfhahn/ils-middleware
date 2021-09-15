"""Test FOLIO Operators and functions."""
from dags.folio import map_to_folio


def test_folio():
    """Test map_to_folio."""
    assert map_to_folio([]) == "folio_send"
