"""POST API function for Alma API"""
import logging
import requests  # type: ignore

from airflow.hooks.base_hook import BaseHook

logger = logging.getLogger(__name__)


def Alma_requests(http_verb, uri, data, headers):
    if http_verb.startswith("post"):
        result = requests.post(uri, data=data, headers=headers)
    else:
        msg = f"{http_verb} not available for {uri}"
        logger.error(msg)
        raise ValueError(msg)
    if result.status_code > 399:
        msg = f"Alma Web Service Call to {uri} Failed with {result.status_code}\n{result.text}"
        logger.error(msg)
        raise Exception(msg)
    return result


def almaRequest(**kwargs) -> str:
    alma_api_key = kwargs.get("alma_api_key")
    # Generated from Alma Admin for connecting to your Alma server. Note Alma Sandbox and Alma Prod generate different keys.
    from_nz_mms_id = kwargs.get("from_nz_mms_id")
    # The MMS_ID of the Network-Zone record. Leave empty when creating a regular local record.
    from_cz_mms_id = kwargs.get("from_cz_mms_id")
    # The MMS_ID of the Community-Zone record. Leave empty when creating a regular local record.
    normalization = kwargs.get("normalization")
    # The id of the normalization profile to run.
    validate = kwargs.get("validate")
    # Indicating whether to check for errors. Default: false.
    override_warning = kwargs.get("override_warning")
    # Indicating whether to ignore warnings.
    # Default: true (record will be saved and the warnings will be added to the API output).
    check_match = kwargs.get("check_match")
    # Indicating whether to check for a match. Default: false (record will be saved despite possible match).
    import_profile = kwargs.get("import_profile")
    # The id of the Import Profile to use when processing the input record.
    # Note that according to the profile configuration, the API can update an existing record in some cases.
    conn_id = kwargs.get("conn_id", "")
    data = kwargs.get("data")
    http_verb = kwargs.get("http_verb", "post")
    headers = {
        "Content-Type": "application/xml",
        "x-api-key": alma_api_key,
    }

    logger.info(f"Headers {headers}")
    # Generate Alma URL based on the Airflow Connection and endpoint
    alma_conn = BaseHook.get_connection(conn_id)
    # {{baseUrl}}/almaws/v1/bibs?from_nz_mms_id=&from_cz_mms_id=&normalization=&validate=false&override_warning=true
    # &check_match=false&import_profile=&apikey=<API Key>
    alma_uri = (
        alma_conn.host
        + from_nz_mms_id
        + from_cz_mms_id
        + normalization
        + validate
        + override_warning
    )
    +check_match + import_profile + alma_api_key

    alma_result = Alma_requests(http_verb, alma_uri, data, headers)

    logger.debug(
        f"Alma Results alma_result {alma_result.status_code}\n{alma_result.text}"
    )
    return (
        alma_result.text
    )  # todo: parse the XML response for MMSID element to send back into Sinopia description(s)
