"""FOLIO Operators and Functions for Institutional DAGs."""
import json
import requests

from airflow.providers.http.operators.http import SimpleHttpOperator


def FolioLogin(**kwargs) -> SimpleHttpOperator:
    """Logs into FOLIO and returns Okapi token."""
    username = kwargs["username"]
    password = kwargs["password"]

    url = "https://okapi-folio.dev.sul.stanford.edu/authn/login"
    data = {"username": username, "password": password}
    headers = {"Content-type": "application/json", "x-okapi-tenant": "sul"}

    result = requests.post(url, data=json.dumps(data), headers=headers)

    if result.status_code == 201:  # Valid token created and returned
        return result.headers.get("x-okapi-token")

    result.raise_for_status()
