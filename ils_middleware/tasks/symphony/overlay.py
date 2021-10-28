"""Overlays an existing Symphony record"""

import ast
import datetime
import json
import logging

from ils_middleware.tasks.symphony.request import SymphonyRequest

logger = logging.getLogger(__name__)


def overlay_marc_in_symphony(*args, **kwargs) -> str:
    """Overlays an existing record in Symphony"""
    marc_json = kwargs.get("marc_json", "")
    catkey = kwargs.get("catkey")

    if not catkey:
        msg = "Catalog ID is required"
        logger.error(msg)
        raise ValueError(msg)

    marc_json = ast.literal_eval(marc_json)

    payload = {
        "@resource": "/catalog/bib",
        "@key": catkey,
        "catalogDate": datetime.datetime.now().strftime("%Y-%m-%d"),
        "bib": marc_json,
    }

    return SymphonyRequest(
        **kwargs,
        data=json.dumps(payload),
        http_verb="put",
        endpoint=f"catalog/bib/key/{catkey}",
        filter=lambda response: response.json().get("systemModifiedDate"),
    )
