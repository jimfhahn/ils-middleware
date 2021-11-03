"""Test Sinopia email notification sending."""
import pytest
from pytest_mock import MockerFixture
from datetime import datetime

from airflow import DAG

from airflow.providers.amazon.aws.hooks.ses import SESHook
from airflow.models.taskinstance import TaskInstance
from airflow.operators.dummy import DummyOperator

from ils_middleware.tasks.sinopia.email import (
    send_update_success_emails,
    send_task_failure_notifications,
    honeybadger,  # for spying on notifications
    logger,  # for spying on logging
)


mock_messages_list = [
    {
        "MessageId": "1234ab4f-52ab-4b54-a6cd-098b4321f02a",
        "ReceiptHandle": "real+long+string",
        "MD5OfBody": "8bf1ff18a27bba7cc54fc68dc3ca9d5a",
        "Body": '{"resource":{"uri":"http://exmpl/resource/foo"},"user":{"email":"abc@yale.edu"},"group":"stanford","target":"ils"}',  # noqa: E501 line too long
    },
    {
        "MessageId": "5678ab4f-52ab-4b54-a6cd-765b9876f08a",
        "ReceiptHandle": "real+long+string2",
        "MD5OfBody": "388e4db0aacfbcbe93e6072dd7cda916",
        "Body": '{"resource":{"uri":"http://exmpl/resource/bar"},"user":{"email":"abc@yale.edu"},"group":"yale","target":"ils"}',  # noqa: E501 line too long
    },
]


def test_task():
    start_date = datetime(2021, 9, 20)
    test_dag = DAG(
        "test_dag", default_args={"owner": "airflow", "start_date": start_date}
    )
    return DummyOperator(task_id="test", dag=test_dag)


@pytest.fixture
def mock_success_task_instance(monkeypatch):
    def mock_xcom_pull(*args, **kwargs) -> list:
        return [mock_messages_list]

    monkeypatch.setattr(TaskInstance, "xcom_pull", mock_xcom_pull)


@pytest.fixture
def mock_failure_task_instance(monkeypatch):
    def mock_xcom_pull(*args, **kwargs) -> list:
        if kwargs["key"] == "email":
            return ["user@institution.edu"]
        if kwargs["key"] == "resource_uri":
            return [
                "https://api.sinopia.io/resource/9d3b525e-2d8f-4192-8456-0fb804d34fd1"
            ]

        return [None]

    monkeypatch.setattr(TaskInstance, "xcom_pull", mock_xcom_pull)


@pytest.fixture
def mock_failure_no_user_available_task_instance(monkeypatch):
    def mock_xcom_pull(*args, **kwargs) -> list:
        return []

    monkeypatch.setattr(TaskInstance, "xcom_pull", mock_xcom_pull)


def test_send_update_success_emails(
    mock_success_task_instance, mocker: MockerFixture
) -> None:
    task_instance = TaskInstance(test_task())

    mock_ses_hook_obj = mocker.Mock(SESHook)
    patched_ses_hook_class = mocker.patch(
        "ils_middleware.tasks.sinopia.email.SESHook", return_value=mock_ses_hook_obj
    )
    send_update_success_emails(task_instance=task_instance)

    patched_ses_hook_class.assert_called_once_with(aws_conn_id="aws_ses_dev")
    mock_ses_hook_obj.send_email.assert_any_call(
        **{
            "mail_from": "sinopia-devs@lists.stanford.edu",
            "to": "abc@yale.edu",
            "subject": "successfully published http://exmpl/resource/foo",
            "html_content": "You have successfully published http://exmpl/resource/foo from Sinopia to stanford ils",
        }
    )
    mock_ses_hook_obj.send_email.assert_any_call(
        **{
            "mail_from": "sinopia-devs@lists.stanford.edu",
            "to": "abc@yale.edu",
            "subject": "successfully published http://exmpl/resource/bar",
            "html_content": "You have successfully published http://exmpl/resource/bar from Sinopia to yale ils",
        }
    )
    assert mock_ses_hook_obj.send_email.call_count == 2


def test_send_task_failure_notifications(
    mock_failure_task_instance, mocker: MockerFixture
) -> None:
    execution_date = datetime(2021, 9, 21)
    task_instance = TaskInstance(test_task())

    hb_notify_spy = mocker.spy(honeybadger, "notify")
    logger_spy = mocker.spy(logger, "error")

    mock_ses_hook_obj = mocker.Mock(SESHook)
    patched_ses_hook_class = mocker.patch(
        "ils_middleware.tasks.sinopia.email.SESHook", return_value=mock_ses_hook_obj
    )

    send_task_failure_notifications(
        execution_date=execution_date, task=test_task(), task_instance=task_instance
    )

    expected_kwargs = {
        "execution_date": execution_date,
        "task": test_task(),
        "task_instance": task_instance,
    }
    expected_err_context = {"parent_task_ids": [], "kwargs": expected_kwargs}
    hb_notify_spy.assert_called_once_with(
        "Error executing upstream task",
        context=expected_err_context,
    )
    logger_spy.assert_called_once_with(
        f"Error executing upstream task: err_msg_context={expected_err_context}"
    )

    patched_ses_hook_class.assert_called_once_with(aws_conn_id="aws_ses_dev")
    expected_exec_date_str = "2021-09-21 00:00:00"
    expected_uri = (
        "https://api.sinopia.io/resource/9d3b525e-2d8f-4192-8456-0fb804d34fd1"
    )
    mock_ses_hook_obj.send_email.assert_called_once_with(
        **{
            "mail_from": "sinopia-devs@lists.stanford.edu",
            "to": ["user@institution.edu"],
            "subject": "Error executing Sinopia to ILS task on your behalf",
            "html_content": f"execution_date: {expected_exec_date_str} / resource_uri (if available): ['{expected_uri}'] / group (if available): [None]",  # noqa: E501 line too long
        }
    )
    assert mock_ses_hook_obj.send_email.call_count == 1


def test_send_task_failure_notifications_no_user_available(
    mock_failure_no_user_available_task_instance, mocker: MockerFixture
) -> None:
    execution_date = datetime(2021, 9, 21)
    task_instance = TaskInstance(test_task())

    hb_notify_spy = mocker.spy(honeybadger, "notify")
    logger_spy = mocker.spy(logger, "error")

    mock_ses_hook_obj = mocker.Mock(SESHook)
    patched_ses_hook_class = mocker.patch(
        "ils_middleware.tasks.sinopia.email.SESHook", return_value=mock_ses_hook_obj
    )

    send_task_failure_notifications(
        execution_date=execution_date, task=test_task(), task_instance=task_instance
    )

    expected_kwargs = {
        "execution_date": execution_date,
        "task": test_task(),
        "task_instance": task_instance,
    }
    expected_err_context = {"parent_task_ids": [], "kwargs": expected_kwargs}
    hb_notify_spy.assert_any_call(
        "Error executing upstream task",
        context=expected_err_context,
    )
    logger_spy.assert_called_once_with(
        f"Error executing upstream task: err_msg_context={expected_err_context}"
    )
    hb_notify_spy.assert_any_call(
        "Unable to determine user to notify for task failure",
        context=expected_err_context,
    )
    assert hb_notify_spy.call_count == 2

    assert patched_ses_hook_class.call_count == 0
    assert mock_ses_hook_obj.send_email.call_count == 0
