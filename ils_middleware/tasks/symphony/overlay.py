"""Overlays an existing Symphony record"""

import datetime
import json
import logging

from ils_middleware.tasks.symphony.request import SymphonyRequest

logger = logging.getLogger(__name__)


def overlay_marc_in_symphony(*args, **kwargs):
    """Overlays an existing record in Symphony"""
    task_instance = kwargs.get("task_instance")
    resources = task_instance.xcom_pull(
        key="overlay_resources", task_ids="process_symphony.new-or-overlay"
    )

    missing_catkeys = []
    for resource in resources:
        resource_uri = resource["resource_uri"]

        if (
            not "catkey" in resource
            or len(resource["catkey"]) < 1
            or resource["catkey"][0].get("SIRSI") is None
        ):
            msg = f"Catalog ID is required for {resource_uri}"
            missing_catkeys.append(resource_uri)
            logger.error(msg)
            continue
        else:
            catkey = resource["catkey"][0].get("SIRSI")

        marc_json = task_instance.xcom_pull(
            key=resource_uri, task_ids="process_symphony.convert_to_symphony_json"
        )

        payload = {
            "@resource": "/catalog/bib",
            "@key": catkey,
            "catalogDate": datetime.datetime.now().strftime("%Y-%m-%d"),
            "bib": marc_json,
        }

        task_instance.xcom_push(
            key=resource_uri,
            value=SymphonyRequest(
                **kwargs,
                data=json.dumps(payload),
                http_verb="put",
                endpoint=f"catalog/bib/key/{catkey}",
                filter=lambda response: response.json().get("@key"),
            ),
        )

    task_instance.xcom_push(key="missing_catkeys", value=missing_catkeys)
