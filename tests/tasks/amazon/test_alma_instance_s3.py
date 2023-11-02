from unittest.mock import Mock, patch
from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # noqa
from ils_middleware.tasks.amazon.alma_instance_s3 import send_instance_to_alma_s3
import ssl


@patch.dict(
    "os.environ", {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"}
)
@patch("airflow.models.Variable.get")
@patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_connection")
@patch("airflow.providers.amazon.aws.hooks.s3.S3Hook")
@patch("rdflib.Graph")
@patch("lxml.etree")
@patch.object(S3Hook, "load_bytes", return_value=None)
def test_send_instance_to_alma_s3(
    mock_load_bytes,
    mock_etree,
    mock_graph,
    mock_s3_hook,
    mock_get_connection,
    mock_variable,
):
    # Arrange
    mock_task_instance = Mock()
    ssl._create_default_https_context = ssl._create_unverified_context
    mock_task_instance.xcom_pull.return_value = [
        "https://api.development.sinopia.io/resource/7d6626a9-45ca-46b7-ba1e-47c322998403"
    ]
    mock_graph_instance = mock_graph.return_value
    mock_graph_instance.serialize.return_value = b"<test></test>"
    mock_graph_instance.parse.return_value = (
        None  # Mock the parse method on the instance
    )
    mock_etree.XSLT.return_value = Mock()  # Mock the XSLT transformation
    mock_etree.fromstring.return_value = Mock()  # Mock the fromstring method
    mock_etree.tostring.return_value = b"<test></test>"  # Mock the tostring method
    mock_variable.return_value = "test_bucket"
    mock_connection = Mock()
    mock_connection.login = "test_access_key_id"
    mock_connection.password = "test_secret_access_key"
    mock_get_connection.return_value = mock_connection
    mock_s3_hook_instance = Mock()
    mock_s3_hook.return_value = mock_s3_hook_instance
    mock_s3_hook.get_connection = Mock(return_value=mock_connection)

    send_instance_to_alma_s3(task_instance=mock_task_instance)
