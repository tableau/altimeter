import boto3
from botocore.exceptions import ClientError
from unittest import TestCase
from moto import mock_ec2, mock_elbv2
from unittest.mock import patch
from altimeter.aws.resource.elbv2.target_group import TargetGroupResourceSpec
from altimeter.aws.scan.aws_accessor import AWSAccessor


class TestTargetGroup(TestCase):
    @mock_elbv2
    @mock_ec2
    def test_disappearing_target_group_race_condition(self):
        account_id = "123456789012"
        region_name = "us-east-1"
        tg_name = "foo"

        session = boto3.Session()

        client = session.client("elbv2", region_name=region_name)

        resp = client.create_target_group(Name=tg_name, Port=443)
        tg_arn = resp["TargetGroups"][0]["TargetGroupArn"]

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        with patch(
            "altimeter.aws.resource.elbv2.target_group.get_target_group_health"
        ) as mock_get_target_group_health:
            mock_get_target_group_health.side_effect = ClientError(
                operation_name="DescribeTargetHealth",
                error_response={
                    "Error": {
                        "Code": "TargetGroupNotFound",
                        "Message": f"Target groups '{tg_arn}' not found",
                    }
                },
            )
            resources = TargetGroupResourceSpec.scan(scan_accessor=scan_accessor)
            self.assertEqual(resources, [])
