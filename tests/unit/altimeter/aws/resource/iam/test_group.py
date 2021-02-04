import boto3
from botocore.exceptions import ClientError
from unittest import TestCase
from moto import mock_iam
from unittest.mock import patch
from altimeter.aws.resource.iam.group import IAMGroupResourceSpec
from altimeter.aws.scan.aws_accessor import AWSAccessor


class TestIAMGroup(TestCase):
    @mock_iam
    def test_disappearing_group_race_condition(self):
        account_id = "123456789012"
        group_name = "foo"
        region_name = "us-east-1"

        session = boto3.Session()

        client = session.client("iam")

        client.create_group(GroupName=group_name)

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        with patch(
            "altimeter.aws.resource.iam.group.IAMGroupResourceSpec.get_group_users"
        ) as mock_get_group_users:
            mock_get_group_users.side_effect = ClientError(
                operation_name="GetGroup",
                error_response={
                    "Error": {
                        "Code": "NoSuchEntity",
                        "Message": f"The group with name {group_name} cannot be found.",
                    }
                },
            )
            resources = IAMGroupResourceSpec.scan(scan_accessor=scan_accessor)
            self.assertEqual(resources, [])
