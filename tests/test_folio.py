from dags.folio import map_to_folio


def test_folio():
    assert map_to_folio([]) == "folio_send"
