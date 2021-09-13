import json
import logging
import requests

from airflow.providers.http.operators.http import SimpleHttpOperator


def NewMARCtoSymphony(marc_json) -> SimpleHttpOperator:
    # Should be retrieved via secrets
    app_id = "SINOPIA_DEV"
    client_id = "SymWSStaffClient"
    session_token = "abscdedfe"

    payload = {
        "catalogFormat": {"@resource":"/policy/catalogFormat","@key":"MARC"},
         "shadowed": False,
         "bib": marc_json,
         "callList": [
            {
                "@resource": "/catalog/call",
                "callNumber": "AUTO",
                "classification": {"@resource":"/policy/classification","@key":"LC"},
                "library": {"@resource":"/policy/library","@key":"GREEN"}
            }
         ]
    }

    def filter_catid(response: requests.Response) -> str:
        return response.json().get('@id')


    return SimpleHttpOperator(
        task_id="post_new_symphony",
        endpoint='post',
        data=json.dumps(payload),
        headers={ "Content-Type": "application/vnd.sirsidynix.roa.resource.v2+json",
                  "Accept": "application/vnd.sirsidynix.roa.resource.v2+json",
                  "sd-originating-app-id": app_id,
                  "x-sirs-clientID": client_id,
                  "x-sirs-sessionToken": session_token },
        response_filter=filter_catid,
        data=json.dumps(payload)
    )
