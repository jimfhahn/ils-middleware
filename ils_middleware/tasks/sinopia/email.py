"""Sinopia Operators and Functions for email notifications."""
from airflow.providers.amazon.aws.hooks.ses import SESHook

import json
import logging

from airflow.models.taskinstance import TaskInstance
from honeybadger import honeybadger


logger = logging.getLogger(__name__)


# NOTE: Another approach to consider would be to return an EmailOperator, either by
# using a factory pattern to return a custom operator instance, or by subclassing.
# see:
# https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/email/index.html
# https://airflow.apache.org/docs/apache-airflow/stable/howto/email-config.html?highlight=ses#send-email-using-aws-ses
def send_update_success_emails(**kwargs) -> None:
    task_instance = kwargs["task_instance"]
    ses_hook = SESHook(aws_conn_id="aws_ses_connection")
    for email_attributes in _email_on_success_info_list(task_instance):
        ses_hook.send_email(**email_attributes)


def send_task_failure_notifications(**kwargs) -> None:
    parent_task_ids = list(kwargs["task"].upstream_task_ids)

    err_msg_context = {"parent_task_ids": parent_task_ids, "kwargs": kwargs}
    honeybadger.notify("Error executing upstream task", context=err_msg_context)
    logger.error(f"Error executing upstream task: err_msg_context={err_msg_context}")

    task_instance = kwargs["task_instance"]
    user_email = task_instance.xcom_pull(key="email", task_ids=["sqs-message-parse"])
    if len(user_email) > 0:
        _send_task_failure_email(user_email, kwargs, task_instance)
    else:
        honeybadger.notify(
            "Unable to determine user to notify for task failure",
            context=err_msg_context,
        )


def _send_task_failure_email(
    user_email: str, kwargs: dict, task_instance: TaskInstance
) -> None:
    ses_hook = SESHook(aws_conn_id="aws_ses_connection")
    ses_hook.send_email(
        **_email_on_failure_attributes(user_email, kwargs, task_instance)
    )


def _email_on_failure_attributes(
    user_email: str, kwargs: dict, task_instance: TaskInstance
) -> dict:
    execution_date = kwargs["execution_date"]
    resource_uri = task_instance.xcom_pull(
        key="resource_uri", task_ids=["sqs-message-parse"]
    )
    group = task_instance.xcom_pull(key="group", task_ids=["sqs-message-parse"])
    email_body = f"execution_date: {execution_date} / resource_uri (if available): {resource_uri} / group (if available): {group}"
    return {
        "mail_from": "sinopia-devs@lists.stanford.edu",
        "to": user_email,
        "subject": "Error executing Sinopia to ILS task on your behalf",
        "html_content": email_body,
    }


def _email_on_success_info_list(task_instance: TaskInstance) -> list:
    raw_sqs_messages = task_instance.xcom_pull(key="messages", task_ids=["sqs-sensor"])[
        0
    ]
    logger.debug(f"raw_sqs_messages: {raw_sqs_messages}")
    return [
        _email_on_success_attributes(raw_sqs_msg) for raw_sqs_msg in raw_sqs_messages
    ]


def _email_on_success_attributes(raw_sqs_message: dict) -> dict:
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
