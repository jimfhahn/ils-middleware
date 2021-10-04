"""FOLIO Operators and Functions for Institutional DAGs."""
import json

from ils_middleware.tasks.folio.request import FolioRequest
from airflow.providers.http.operators.http import SimpleHttpOperator


def FolioLogin(**kwargs) -> SimpleHttpOperator:
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
