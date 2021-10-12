"""Test Sinopia email notification sending."""
import pytest
from pytest_mock import MockerFixture
from datetime import datetime

from airflow import DAG

from airflow.providers.amazon.aws.hooks.ses import SESHook
from airflow.models.taskinstance import TaskInstance
from airflow.operators.dummy import DummyOperator

from ils_middleware.tasks.sinopia.email import email_for_success


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
def mock_task_instance(monkeypatch):
    def mock_xcom_pull(*args, **kwargs):
        return [mock_messages_list]

    monkeypatch.setattr(TaskInstance, "xcom_pull", mock_xcom_pull)


def test_email_for_success(mock_task_instance, mocker: MockerFixture) -> None:
    execution_date = datetime(2021, 9, 21)
    task_instance = TaskInstance(test_task(), execution_date)

    mock_ses_hook_obj = mocker.Mock(SESHook)
    patched_ses_hook_class = mocker.patch(
        "ils_middleware.tasks.sinopia.email.SESHook", return_value=mock_ses_hook_obj
    )
    email_for_success(task_instance=task_instance)

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
