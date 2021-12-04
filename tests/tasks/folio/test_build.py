import pytest

from ils_middleware.tasks.folio.build import _title_transform


def test_title_transform_all():
    title_str = _title_transform([["COVID-19", "Survivors", "California", "1st"]])
    assert title_str.startswith("COVID-19 : Survivors. California, 1st")
