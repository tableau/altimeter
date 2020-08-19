import os
from unittest import mock

import boto3
import moto

from altimeter.qj.config import SecurityConfig
from altimeter.qj.security import get_api_key


@moto.mock_secretsmanager
def test_get_api_key():
    with mock.patch.dict(os.environ, {"API_KEY_SECRET_NAME": "test-api-key"}):
        client = boto3.client("secretsmanager", region_name="us-west-2")
        client.create_secret(
            Name=SecurityConfig().api_key_secret_name, SecretString="testvalue123",
        )
        api_key = get_api_key(region_name="us-west-2")
        assert api_key == "testvalue123"
