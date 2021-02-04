import boto3
from botocore.exceptions import ClientError
from unittest import TestCase
from moto import mock_iam
from unittest.mock import patch
from altimeter.aws.resource.iam.role import IAMRoleResourceSpec
from altimeter.aws.scan.aws_accessor import AWSAccessor


class TestIAMRole(TestCase):
    @mock_iam
    def test_disappearing_role_race_condition(self):
        account_id = "123456789012"
        role_name = "foo"
        region_name = "us-east-1"

        session = boto3.Session()
        client = session.client("iam")
        client.create_role(RoleName=role_name, AssumeRolePolicyDocument="{}")

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        with patch(
            "altimeter.aws.resource.iam.role.get_attached_role_policies"
        ) as mock_get_group_users:
            mock_get_group_users.side_effect = ClientError(
                operation_name="ListAttachedRolePolicies",
                error_response={
                    "Error": {
                        "Code": "NoSuchEntity",
                        "Message": f"The role with name {role_name} cannot be found.",
                    }
                },
            )
            resources = IAMRoleResourceSpec.scan(scan_accessor=scan_accessor)
            self.assertEqual(resources, [])
