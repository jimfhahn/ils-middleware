"""POST API function for Alma API"""
import logging
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from urllib.parse import urlparse
from os import path
import requests
import lxml.etree as ET

logger = logging.getLogger(__name__)

# Penn's Alma Sandbox API
# Alma API uses the same path to either create new or match and is handled with
# Alma Import Profile, setup in Alma admin side. The pre-configured Import Profile
# can be specified with an import ID to handle the import.


def NewMARCtoAlma(**kwargs):
    s3_hook = S3Hook(aws_conn_id="aws_lambda_connection")
    task_instance = kwargs.get("task_instance")
    resources = task_instance.xcom_pull(key="resources", task_ids="sqs-message-parse")

    for instance_uri in resources:
        instance_path = urlparse(instance_uri).path
        instance_id = path.split(instance_path)[-1]

    s3_hook.download_file(
        key=f"marc/airflow/{instance_id}/alma.xml",
        bucket_name=Variable.get("marc_s3_bucket"),
    )

    with open("alma.xml", "rb") as f:
        data = f.read()
        logger.debug(f"file data: {data}")

    alma_api_key = Variable.get("alma_sandbox_api_key")
    alma_import_profile_id = Variable.get("import_profile_id")
    # The id of the Import Profile to use when processing the input record.
    # Note that according to the profile configuration, the API can update an existing record in some cases.
    # push the data to the next task
    alma_uri = (
        "https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs?"
        + "from_nz_mms_id=&from_cz_mms_id=&normalization=&validate=false"
        + "&override_warning=true&check_match=false&import_profile="
        + alma_import_profile_id
        + "&apikey="
        + alma_api_key
    )
    # post to alma
    alma_result = requests.post(
        alma_uri,
        headers={
            "Content-Type": "application/xml; charset=utf-8",
            "Accept": "application/xml",
            "x-api-key": alma_api_key,
        },
        data=data,
    )
    logger.debug(f"alma result: {alma_result.status_code}\n{alma_result.text}")

    # push the mms_id from the Alma API result to the next task, Sinopia Metadata Update
    result = alma_result.content
    xml_response = ET.fromstring(result)
    mms_id = xml_response.xpath("//mms_id/text()")
    logger.debug(f"mms_id: {mms_id}")
    task_instance.xcom_push(key="mms_id", value=mms_id)
