import json
import pytest


@pytest.fixture
def mock_airflow_var(mocker):
    mock_variable = mocker.patch(
        "ils_middleware.tasks.amazon.sqs.Variable.get",
        return_value="http://example.queue.edu/all-institutions",
    )
    return mock_variable


messages = [
    {"Body": json.dumps({"group": "stanford"})},
    {"Body": json.dumps({"group": "colorado"})},
]


def test_parse_institutional_msgs(mock_airflow_var, mocker):
    from ils_middleware.dags.queue_monitor import _parse_institutional_msgs

    task_instance = mocker.MagicMock()
    task_instance.xcom_pull = lambda **kwargs: messages
    institutional_messages = _parse_institutional_msgs(task_instance)
    assert len(institutional_messages) == 1
    assert institutional_messages[0]["group"] == "stanford"


def test_trigger_dags(mocker, mock_airflow_var, caplog):
    mock_trigger_operator = mocker.patch(
        "ils_middleware.dags.queue_monitor.TriggerDagRunOperator"
    )
    from ils_middleware.dags.queue_monitor import (
        _parse_institutional_msgs,
        _trigger_dags,
    )

    task_instance = mocker.MagicMock()
    task_instance.xcom_pull = lambda **kwargs: messages

    institutional_messages = _parse_institutional_msgs(task_instance)
    _trigger_dags(messages=institutional_messages)

    assert mock_trigger_operator.called
    assert "Trigger DAG for stanford" in caplog.text
