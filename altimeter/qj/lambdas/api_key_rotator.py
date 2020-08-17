"""Lambda to rotate the api key which is stored in secrets manager.

Based on https://github.com/aws-samples/aws-secrets-manager-rotation-lambdas/blob/master/SecretsManagerRotationTemplate/lambda_function.py
"""
import secrets
import string
from typing import Any, Dict

import boto3
from botocore.client import BaseClient

from altimeter.core.log import Logger
from altimeter.qj.client import QJAPIClient
from altimeter.qj.config import APIKeyRotatorConfig
from altimeter.qj.log import QJLogEvents


def lambda_handler(event: Dict[str, str], _: Dict[str, Any]) -> None:
    """Lambda to rotate the api key which is stored in secrets manager"""
    arn = event["SecretId"]
    token = event["ClientRequestToken"]
    step = event["Step"]
    config = APIKeyRotatorConfig()
    logger = Logger()

    # Setup the client
    sm_client = boto3.client("secretsmanager", region_name=config.region)

    # Make sure the version is staged correctly
    metadata = sm_client.describe_secret(SecretId=arn)
    if not metadata["RotationEnabled"]:
        raise ValueError(f"Secret {arn} is not enabled for rotation")
    versions = metadata["VersionIdsToStages"]
    if token not in versions:
        raise ValueError(f"Secret version {token} has no stage for rotation of secret {arn}")
    if "AWSCURRENT" in versions[token]:
        logger.info(event=QJLogEvents.SecretVersionAlreadyCurrent, arn=arn, token=token)
        return
    if "AWSPENDING" not in versions[token]:
        raise ValueError("Secret version token not set as AWSPENDING for rotation of secret arn.")

    if step == "createSecret":
        create_secret(sm_client, arn, token)
    elif step == "setSecret":
        pass
    elif step == "testSecret":
        test_secret(sm_client, arn, token, config)
    elif step == "finishSecret":
        finish_secret(sm_client, arn, token)
    else:
        raise ValueError(f"Invalid step parameter {step}")


def create_secret(sm_client: BaseClient, arn: str, token: str) -> None:
    """Create the secret. First check for the existence of a secret for the passed in token. If one
    does not exist, it will generate a new secret and put it with the passed in token."""
    logger = Logger()
    # Try to get the PENDING secret version, if that fails, put a new secret
    try:
        sm_client.get_secret_value(SecretId=arn, VersionId=token, VersionStage="AWSPENDING")
        logger.info(event=QJLogEvents.SecretRetrieved, arn=arn, token=token)
    except sm_client.exceptions.ResourceNotFoundException:
        # Generate a random api key
        api_key = "".join(
            secrets.choice(string.ascii_uppercase + string.digits) for _ in range(512)
        )
        # Put the secret
        sm_client.put_secret_value(
            SecretId=arn,
            ClientRequestToken=token,
            SecretString=api_key,
            VersionStages=["AWSPENDING"],
        )
        logger.info(event=QJLogEvents.SecretCreated, arn=arn, token=token)


def test_secret(sm_client: BaseClient, arn: str, token: str, config: APIKeyRotatorConfig) -> None:
    """Test the secret"""
    resp = sm_client.get_secret_value(SecretId=arn, VersionId=token,)
    api_key = resp["SecretString"]
    qj_api_client = QJAPIClient(host=config.api_host, port=config.api_port, api_key=api_key)
    qj_api_client.get_auth()


def finish_secret(sm_client: BaseClient, arn: str, token: str) -> None:
    """This method finalizes the rotation process by marking the secret version passed in as the
    AWSCURRENT secret."""
    logger = Logger()
    # First describe the secret to get the current version
    metadata = sm_client.describe_secret(SecretId=arn)
    current_version = None
    for version in metadata["VersionIdsToStages"]:
        if "AWSCURRENT" in metadata["VersionIdsToStages"][version]:
            if version == token:
                # The correct version is already marked as current, return
                logger.info(event=QJLogEvents.SecretAlreadyCurrent, arn=arn, token=token)
                return
            current_version = version
            break
    # Finalize by staging the secret version current
    sm_client.update_secret_version_stage(
        SecretId=arn,
        VersionStage="AWSCURRENT",
        MoveToVersionId=token,
        RemoveFromVersionId=current_version,
    )
    logger.info(event=QJLogEvents.SecretSetCurrent, arn=arn, token=token)
