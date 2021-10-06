"""Sinopia Operators and Functions for email notifications."""
from airflow.providers.amazon.aws.hooks.ses import SESHook

import json
import logging

logger = logging.getLogger(__name__)


# NOTE: Another approach to consider would be to return an EmailOperator, either by
# using a factory pattern to return a custom operator instance, or by subclassing.
# see:
# https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/email/index.html
# https://airflow.apache.org/docs/apache-airflow/stable/howto/email-config.html?highlight=ses#send-email-using-aws-ses
def email_for_success(**kwargs) -> None:
    task_instance = kwargs["task_instance"]
    sinopia_env = kwargs.get("sinopia_env", "dev")
    for email_attributes in _email_info_list(task_instance):
        SESHook(aws_conn_id=f"aws_ses_{sinopia_env}").send_email(**email_attributes)


def _email_info_list(task_instance) -> list:
    raw_sqs_messages = task_instance.xcom_pull(key="messages", task_ids=["sqs-sensor"])[
        0
    ]
    logger.debug(f"raw_sqs_messages: {raw_sqs_messages}")
    return [_email_attributes(raw_sqs_msg) for raw_sqs_msg in raw_sqs_messages]


def _email_attributes(raw_sqs_message: dict) -> dict:
    logger.debug(f"raw_sqs_message: {raw_sqs_message}")
    parsed_msg_body = json.loads(raw_sqs_message["Body"])
    email_addr = parsed_msg_body["user"]["email"]
    resource_uri = parsed_msg_body["resource"]["uri"]
    group = parsed_msg_body["group"]
    target = parsed_msg_body["target"]
    return {
        "mail_from": "sinopia-devs@lists.stanford.edu",
        "to": email_addr,
        "subject": f"successfully published {resource_uri}",
        "html_content": f"You have successfully published {resource_uri} from Sinopia to {group} {target}",
    }
