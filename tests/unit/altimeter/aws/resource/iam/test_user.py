import boto3
from botocore.exceptions import ClientError
from unittest import TestCase
from moto import mock_iam
from unittest.mock import patch
from altimeter.aws.resource.iam.user import IAMUserResourceSpec
from altimeter.aws.scan.aws_accessor import AWSAccessor


class TestIAMUser(TestCase):
    @mock_iam
    def test_disappearing_user_race_condition_get_user_access_keys(self):
        account_id = "123456789012"
        user_name = "foo"
        region_name = "us-east-1"

        session = boto3.Session()
        client = session.client("iam")
        client.create_user(UserName=user_name)

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        with patch(
            "altimeter.aws.resource.iam.user.IAMUserResourceSpec.get_user_access_keys"
        ) as mock_get_group_users:
            mock_get_group_users.side_effect = ClientError(
                operation_name="ListAccessKeys",
                error_response={
                    "Error": {
                        "Code": "NoSuchEntity",
                        "Message": f"The user with name {user_name} cannot be found.",
                    }
                },
            )
            resources = IAMUserResourceSpec.scan(scan_accessor=scan_accessor)
            self.assertEqual(resources, [])

    @mock_iam
    def test_disappearing_access_key_race_condition(self):
        account_id = "123456789012"
        user_name = "foo"
        region_name = "us-east-1"

        session = boto3.Session()
        client = session.client("iam")
        client.create_user(UserName=user_name)
        client.create_access_key(UserName=user_name)

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        with patch(
            "altimeter.aws.resource.iam.user.IAMUserResourceSpec.get_access_key_last_used"
        ) as mock_get_group_users:
            mock_get_group_users.side_effect = ClientError(
                operation_name="GetAccessKeyLastUsed",
                error_response={"Error": {"Code": "AccessDenied", "Message": "",}},
            )
            resources = IAMUserResourceSpec.scan(scan_accessor=scan_accessor)
            self.assertEqual(len(resources), 1)
            self.assertEqual(resources[0].resource_id, "arn:aws:iam::123456789012:user/foo")

    @mock_iam
    def test_disappearing_user_race_condition_get_user_mfa_devices(self):
        account_id = "123456789012"
        user_name = "foo"
        region_name = "us-east-1"

        session = boto3.Session()
        client = session.client("iam")
        client.create_user(UserName=user_name)

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        with patch(
            "altimeter.aws.resource.iam.user.IAMUserResourceSpec.get_user_mfa_devices"
        ) as mock_get_group_users:
            mock_get_group_users.side_effect = ClientError(
                operation_name="ListMFADevices",
                error_response={
                    "Error": {
                        "Code": "NoSuchEntity",
                        "Message": f"The user with name {user_name} cannot be found.",
                    }
                },
            )
            resources = IAMUserResourceSpec.scan(scan_accessor=scan_accessor)
            self.assertEqual(resources, [])

    @mock_iam
    def test_disappearing_user_race_condition_get_user_login_profile(self):
        account_id = "123456789012"
        user_name = "foo"
        region_name = "us-east-1"

        session = boto3.Session()
        client = session.client("iam")
        client.create_user(UserName=user_name)

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        with patch(
            "altimeter.aws.resource.iam.user.IAMUserResourceSpec.get_user_login_profile"
        ) as mock_get_group_users:
            mock_get_group_users.side_effect = ClientError(
                operation_name="GetLoginProfile",
                error_response={
                    "Error": {
                        "Code": "NoSuchEntity",
                        "Message": f"The user with name {user_name} cannot be found.",
                    }
                },
            )
            resources = IAMUserResourceSpec.scan(scan_accessor=scan_accessor)
            self.assertEqual(resources, [])
