"""Tests functions for modifying JSON to Symphony JSON."""
import pytest

from dags.tasks.symphony.mod_json import to_symphony_json


@pytest.fixture
def org_json():
    return {
        "leader": "01176nam a2200241uu 4500",
        "fields": [
            {"008": "200915s1998    uk                  eng||"},
            {
                "245": {
                    "ind1": "1",
                    "ind2": "0",
                    "subfields": [
                        {"a": "Hildegard von Bingen's Physica"},
                        {
                            "b": "the complete English translation of "
                            "her classic work on health and "
                            "healing"
                        },
                        {
                            "c": "translated from the Latin by "
                            "Priscilla Throop ; illustrations by "
                            "Mary Elder Jacobsen"
                        },
                    ],
                }
            },
        ],
    }


def test_to_symphony_json(org_json):
    symphony_json = to_symphony_json(org_json)
    assert symphony_json["standard"].startswith("MARC21")
    assert symphony_json["leader"].startswith("01176nam a2200241uu 4500")
    assert symphony_json["fields"][0]["tag"] == "008"
    assert symphony_json["fields"][1]["inds"] == "10"
    assert symphony_json["fields"][1]["subfields"][0]["code"] == "a"
    assert symphony_json["fields"][1]["subfields"][0]["data"].startswith(
        "Hildegard von Bingen's Physica"
    )
