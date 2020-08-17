"""Security related functions"""
import boto3

from altimeter.qj.config import SecurityConfig


def get_api_key(region_name: str, version_stage: str = "AWSCURRENT") -> str:
    """Get the current API key from SecretsManager"""
    security_config = SecurityConfig()
    sm_client = boto3.client("secretsmanager", region_name=region_name)
    resp = sm_client.get_secret_value(
        SecretId=security_config.api_key_secret_name, VersionStage=version_stage
    )
    api_key_secret = resp["SecretString"]
    return api_key_secret
