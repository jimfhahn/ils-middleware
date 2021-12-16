"""Test FOLIO Operators and functions."""

import pytest
import requests

from pytest_mock import MockerFixture

from ils_middleware.tasks.folio.login import FolioLogin


@pytest.fixture
def mock_request(monkeypatch, mocker: MockerFixture):
    def mock_post(*args, **kwargs):
        post_response = mocker.stub(name="post_result")
        post_response.status_code = 201
        post_response.headers = {"x-okapi-token": "some_jwt_token"}

        return post_response

    def mock_raise_for_status(*args, **kwargs):
        error_response = mocker.stub(name="post_error")
        error_response.status_code = 500
        error_response.text = "Internal server error"

    monkeypatch.setattr(requests, "post", mock_post)
    monkeypatch.setattr(requests.Response, "raise_for_status", mock_raise_for_status)


# <Response [201]>
def test_valid_login(mock_request):
    assert (
        FolioLogin(
            url="https://okapi-folio.dev.sul.stanford.edu/authn/login",
            username="DEVSYS",
            password="APASSWord",
            tenant="sul",
        )
        == "some_jwt_token"
    )


def test_missing_url():
    with pytest.raises(KeyError, match="url"):
        FolioLogin()


def test_missing_username():
    with pytest.raises(KeyError, match="username"):
        FolioLogin(url="https://test-login.com")


def test_missing_password():
    with pytest.raises(KeyError, match="password"):
        FolioLogin(url="https://test-login.com", username="DEVSYS")


def test_missing_tenant():
    with pytest.raises(KeyError, match="tenant"):
        FolioLogin(url="https://test-login.com", username="DEVSYS", password="PASS")
