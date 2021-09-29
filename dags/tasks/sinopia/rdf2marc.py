import logging
import json
from urllib.parse import urlparse
from os import path, getenv

from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook


def Rdf2Marc(**kwargs):
    """Runs rdf2marc on a BF Instance URL"""
    task_instance = kwargs["task_instance"]
    instance_uri = task_instance.xcom_pull(task_ids="sqs-sensor")

    instance_path = urlparse(instance_uri).path
    instance_id = path.split(instance_path)[-1]

    sinopia_env = kwargs.get("sinopia_env", "dev")
    rdf2marc_lambda = f"{getenv('RDF2MARC_LAMBDA')}_{sinopia_env.upper()}"
    s3_bucket = f"{getenv('MARC_S3_BUCKET')}_{sinopia_env.upper()}"
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
    print(f"RESULT = {result['StatusCode']}")

    if result["StatusCode"] == 200:
        return instance_id

    logging.error(
        f"RDF2MARC conversion failed for {instance_uri}: {result['FunctionError']}"
    )
    raise Exception()
