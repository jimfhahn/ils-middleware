"""Test FOLIO Mapping from Sinopia."""

from ils_middleware.tasks.folio.map import map_to_folio


def test_folio():
    """Test map_to_folio."""
    assert map_to_folio("http://api.sinopia.io/resource/234455") == "folio_send"
