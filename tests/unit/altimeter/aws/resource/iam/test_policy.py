import boto3
from botocore.exceptions import ClientError
import json
from unittest import TestCase
from moto import mock_iam
from unittest.mock import patch
from altimeter.aws.resource.iam.policy import IAMPolicyResourceSpec
from altimeter.aws.scan.aws_accessor import AWSAccessor


class TestIAMPolicy(TestCase):
    @mock_iam
    def test_disappearing_policy_race_condition(self):
        account_id = "123456789012"
        policy_name = "foo"
        region_name = "us-east-1"

        session = boto3.Session()

        client = session.client("iam")

        policy_json = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "logs:CreateLogGroup", "Resource": "*"}],
        }
        policy_resp = client.create_policy(
            PolicyName=policy_name, PolicyDocument=json.dumps(policy_json),
        )
        policy_arn = policy_resp["Policy"]["Arn"]

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        with patch(
            "altimeter.aws.resource.iam.policy.IAMPolicyResourceSpec.get_policy_version_document_text"
        ) as mock_get_policy_version_document_text:
            mock_get_policy_version_document_text.side_effect = ClientError(
                operation_name="GetPolicyVersion",
                error_response={
                    "Error": {
                        "Code": "NoSuchEntity",
                        "Message": f"Policy {policy_arn} version v1 does not exist or is not attachable.",
                    }
                },
            )
            resources = IAMPolicyResourceSpec.scan(scan_accessor=scan_accessor)
            self.assertEqual(resources, [])
