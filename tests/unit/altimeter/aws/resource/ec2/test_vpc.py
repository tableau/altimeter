from unittest import TestCase

import boto3
from moto import mock_ec2

from altimeter.aws.resource.ec2.vpc import VPCResourceSpec
from altimeter.aws.scan.aws_accessor import AWSAccessor


class TestVPCResourceSpec(TestCase):
    @mock_ec2
    def test_scan(self):
        account_id = "123456789012"
        region_name = "us-east-1"

        session = boto3.Session()

        ec2_client = session.client("ec2", region_name=region_name)
        ec2_client.create_vpc(CidrBlock="10.0.0.0/16")

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        scan_result = VPCResourceSpec.scan(scan_accessor=scan_accessor)
        scan_result_dict = scan_result.to_dict()

        expected_scan_result_dict = {
            "resources": [
                {
                    "type": "aws:ec2:vpc",
                    "links": [
                        {"pred": "is_default", "obj": True, "type": "simple"},
                        {
                            "pred": "cidr_block",
                            "obj": "172.31.0.0/16",
                            "type": "simple",
                        },  # from moto
                        {"pred": "state", "obj": "available", "type": "simple"},
                        {
                            "pred": "account",
                            "obj": "arn:aws::::account/123456789012",
                            "type": "resource_link",
                        },
                        {
                            "pred": "region",
                            "obj": "arn:aws:::123456789012:region/us-east-1",
                            "type": "resource_link",
                        },
                    ],
                },
                {
                    "type": "aws:ec2:vpc",
                    "links": [
                        {"pred": "is_default", "obj": False, "type": "simple"},
                        {"pred": "cidr_block", "obj": "10.0.0.0/16", "type": "simple"},
                        {"pred": "state", "obj": "available", "type": "simple"},
                        {
                            "pred": "account",
                            "obj": "arn:aws::::account/123456789012",
                            "type": "resource_link",
                        },
                        {
                            "pred": "region",
                            "obj": "arn:aws:::123456789012:region/us-east-1",
                            "type": "resource_link",
                        },
                    ],
                },
            ],
            "stats": {
                "count": 1,
                "123456789012": {
                    "count": 1,
                    "us-east-1": {"count": 1, "ec2": {"count": 1, "DescribeVpcs": {"count": 1}}},
                },
            },
            "errors": [],
        }

        self.assertDictEqual(scan_result_dict, expected_scan_result_dict)
