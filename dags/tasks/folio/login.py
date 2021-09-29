"""FOLIO Operators and Functions for Institutional DAGs."""
import json

from .request import FolioRequest


def FolioLogin(**kwargs) -> FolioRequest:
    """Logs into FOLIO and returns Okapi token."""
    username = kwargs.get("username")
    password = kwargs.get("password")

    return FolioRequest(
        **kwargs,
        task_id="folio_login",
        endpoint="authn/login",
        response_filter=lambda response: response.json().get("x-okapi-token"),
        data=json.dumps({"username": username, "password": password}),
    )
