from unittest import TestCase

import boto3
from moto import mock_ec2

from altimeter.aws.resource.ec2.vpc import VPCResourceSpec
from altimeter.aws.scan.aws_accessor import AWSAccessor
from altimeter.core.graph.links import LinkCollection, ResourceLink, SimpleLink
from altimeter.core.resource.resource import Resource


class TestVPCResourceSpec(TestCase):
    @mock_ec2
    def test_scan(self):
        account_id = "123456789012"
        region_name = "us-east-1"

        session = boto3.Session()

        ec2_client = session.client("ec2", region_name=region_name)
        list_resp = ec2_client.describe_vpcs()
        present_vpcs = list_resp["Vpcs"]
        self.assertEqual(len(present_vpcs), 1)
        present_vpc_id = present_vpcs[0]["VpcId"]
        present_vpc_arn = f"arn:aws:ec2:us-east-1:123456789012:vpc/{present_vpc_id}"

        create_resp = ec2_client.create_vpc(CidrBlock="10.0.0.0/16")
        created_vpc_id = create_resp["Vpc"]["VpcId"]
        created_vpc_arn = f"arn:aws:ec2:us-east-1:123456789012:vpc/{created_vpc_id}"

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        resources = VPCResourceSpec.scan(scan_accessor=scan_accessor)

        expected_resources = [
            Resource(
                resource_id=present_vpc_arn,
                type="aws:ec2:vpc",
                link_collection=LinkCollection(
                    simple_links=(
                        SimpleLink(pred="is_default", obj=True),
                        SimpleLink(pred="cidr_block", obj="172.31.0.0/16"),
                        SimpleLink(pred="state", obj="available"),
                    ),
                    resource_links=(
                        ResourceLink(pred="account", obj="arn:aws::::account/123456789012"),
                        ResourceLink(pred="region", obj="arn:aws:::123456789012:region/us-east-1"),
                    ),
                ),
            ),
            Resource(
                resource_id=created_vpc_arn,
                type="aws:ec2:vpc",
                link_collection=LinkCollection(
                    simple_links=(
                        SimpleLink(pred="is_default", obj=False),
                        SimpleLink(pred="cidr_block", obj="10.0.0.0/16"),
                        SimpleLink(pred="state", obj="available"),
                    ),
                    resource_links=(
                        ResourceLink(pred="account", obj="arn:aws::::account/123456789012"),
                        ResourceLink(pred="region", obj="arn:aws:::123456789012:region/us-east-1"),
                    ),
                ),
            ),
        ]
        self.assertEqual(resources, expected_resources)
