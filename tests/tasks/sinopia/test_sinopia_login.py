"""Tests Sinopia Login Function."""

import pytest

import botocore.session
from botocore.stub import Stubber


from airflow.models import Variable

from ils_middleware.tasks.sinopia.login import sinopia_login


@pytest.fixture
def mock_variable(monkeypatch):
    def mock_get(key, default=None):
        if key == "sinopia_user":
            return "ils_middleware"

        if key == "sinopia_password":
            return "a_pwd_1234"

        if key == "cognito_client_id":
            return "abcd3445efg"

    monkeypatch.setattr(Variable, "get", mock_get)


def test_sinopia_login(mock_variable):
    cognito_idp = botocore.session.get_session().create_client(
        "cognito-idp", "us-west-1"
    )
    stubber = Stubber(cognito_idp)

    response = {"AuthenticationResult": {"AccessToken": "abc12345"}}

    stubber.add_response("initiate_auth", response)
    stubber.activate()

    jwt = sinopia_login(client=cognito_idp)
    assert jwt == "abc12345"
