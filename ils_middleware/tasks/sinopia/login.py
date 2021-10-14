"""Sinopia Login to generate a Cognito JWT."""
import boto3
from airflow.models import Variable


def sinopia_login(**kwargs):
    """Log into Sinopia using Airflow Variables."""
    region = kwargs.get("aws_region", "us-west-1")
    sinopia_env = kwargs.get("sinopia_env", "dev")
    sinopia_user = Variable.get("sinopia_user")
    sinopia_password = Variable.get("sinopia_password")
    cognito_app_client_id = Variable.get(f"{sinopia_env}_cognito_client_id")

    client = kwargs.get("client", boto3.client("cognito-idp", region))

    login_response = client.initiate_auth(
        AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": sinopia_user, "PASSWORD": sinopia_password},
        ClientId=cognito_app_client_id,
    )

    return login_response.get("AuthenticationResult").get("AccessToken")
