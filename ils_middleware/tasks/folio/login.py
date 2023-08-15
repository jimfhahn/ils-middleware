"""FOLIO Operators and Functions for Institutional DAGs."""
import logging
import requests

from typing import Union

logger = logging.getLogger(__name__)


def FolioLogin(**kwargs) -> Union[str | None]:  # type: ignore
    """Logs into FOLIO and returns Okapi token."""
    url = kwargs["url"]
    username = kwargs["username"]
    password = kwargs["password"]
    tenant = kwargs["tenant"]

    data = {"username": username, "password": password}
    headers = {"Content-type": "application/json", "x-okapi-tenant": tenant}

    result = requests.post(url, json=data, headers=headers)

    if result.status_code == 201:  # Valid token created and returned
        return result.headers.get("x-okapi-token")

    result.raise_for_status()
