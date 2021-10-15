import logging
import json
from urllib.parse import urlparse
from os import path

from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook

logger = logging.getLogger(__name__)


def Rdf2Marc(**kwargs):
    """Runs rdf2marc on a BF Instance URL"""
    instance_uri = kwargs.get("instance_uri")

    instance_path = urlparse(instance_uri).path
    instance_id = path.split(instance_path)[-1]

    rdf2marc_lambda = kwargs.get("rdf2marc_lambda")

    s3_bucket = kwargs.get("s3_bucket")
    s3_record_path = f"airflow/{instance_id}/record"
    marc_path = f"{s3_record_path}.mar"
    marc_text_path = f"{s3_record_path}.txt"
    marc_err_path = f"{s3_record_path}.err"

    lambda_hook = AwsLambdaHook(
        rdf2marc_lambda,
        log_type="None",
        qualifier="$LATEST",
        invocation_type="RequestResponse",
        config=None,
        aws_conn_id="aws_lambda_connection",
    )

    params = {
        "instance_uri": instance_uri,
        "bucket": s3_bucket,
        "marc_path": marc_path,
        "marc_txt_path": marc_text_path,
        "error_path": marc_err_path,
    }

    result = lambda_hook.invoke_lambda(payload=json.dumps(params))

    logger.debug(f"RESULT = {result['StatusCode']}")

    payload = json.loads(result["Payload"].read())

    if "x-amz-function-error" in result["ResponseMetadata"].get("HTTPHeaders"):
        msg = f"RDF2MARC conversion failed for {instance_uri}, error: {payload.get('errorMessage')}"
        logger.error(msg)

        raise Exception(msg)

    if result["StatusCode"] == 200:
        return instance_id

    logger.error(
        f"RDF2MARC conversion failed for {instance_uri}: {result['FunctionError']}"
    )
    raise Exception()
