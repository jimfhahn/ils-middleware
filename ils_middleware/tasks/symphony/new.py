"""New Record in Symphony"""
import ast
import json

from ils_middleware.tasks.symphony.request import SymphonyRequest


def NewMARCtoSymphony(**kwargs) -> str:
    """Creates a new record in Symphony and returns the new CatKey"""
    marc_json = kwargs.get("marc_json", "")
    library_key = kwargs.get("library_key")
    item_type = kwargs.get("item_type")
    home_location = kwargs.get("home_location")

    marc_json = ast.literal_eval(marc_json)

    payload = {
        "@resource": "/catalog/bib",
        "catalogFormat": {"@resource": "/policy/catalogFormat", "@key": "MARC"},
        "shadowed": False,
        "bib": marc_json,
        "callList": [
            {
                "@resource": "/catalog/call",
                "callNumber": "AUTO",
                "classification": {"@resource": "/policy/classification", "@key": "LC"},
                "library": {"@resource": "/policy/library", "@key": f"{library_key}"},
                "itemList": [
                    {
                        "@resource": "/catalog/item",
                        "barcode": "AUTO",
                        "itemType": {
                            "@resource": "/policy/itemType",
                            "@key": f"{item_type}",
                        },
                        "homeLocation": {
                            "@resource": "/policy/location",
                            "@key": f"{home_location}",
                        },
                    }
                ],
            }
        ],
    }

    return SymphonyRequest(
        **kwargs,
        data=json.dumps(payload),
        endpoint="catalog/bib",
        filter=lambda response: response.json().get("@key"),
    )
