"""Tests functions for modifying JSON to Symphony JSON."""
from ils_middleware.tasks.symphony.mod_json import to_symphony_json

from tasks import test_task_instance, mock_task_instance  # noqa: F401

task_instance = test_task_instance()


def test_to_symphony_json(mock_task_instance):  # noqa: F811
    to_symphony_json(task_instance=task_instance)
    symphony_json = task_instance.xcom_pull(
        key="https://api.development.sinopia.io/resource/0000-1111-2222-3333"
    )
    assert symphony_json["standard"].startswith("MARC21")
    assert symphony_json["leader"].startswith("01455nam a2200277uu 4500")
    assert symphony_json["fields"][0]["tag"] == "003"
    assert symphony_json["fields"][2]["subfields"][0]["code"] == "a"
    assert symphony_json["fields"][2]["subfields"][0]["data"] == "981811"
    assert symphony_json["fields"][7]["subfields"][0]["data"].startswith(
        "Hildegard von Bingen's Physica"
    )
