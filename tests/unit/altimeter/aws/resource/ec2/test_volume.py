from unittest import TestCase

import boto3
from moto import mock_ec2

from altimeter.aws.resource.ec2.volume import EBSVolumeResourceSpec
from altimeter.aws.scan.aws_accessor import AWSAccessor
from altimeter.core.graph.links import LinkCollection, ResourceLink, SimpleLink
from altimeter.core.resource.resource import Resource


class TestEBSVolumeResourceSpec(TestCase):
    @mock_ec2
    def test_scan(self):
        account_id = "123456789012"
        region_name = "us-east-1"

        session = boto3.Session()

        ec2_client = session.client("ec2", region_name=region_name)
        resp = ec2_client.create_volume(Size=1, AvailabilityZone="us-east-1a")
        create_time = resp["CreateTime"]
        created_volume_id = resp["VolumeId"]
        created_volume_arn = f"arn:aws:ec2:us-east-1:123456789012:volume/{created_volume_id}"

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        resources = EBSVolumeResourceSpec.scan(scan_accessor=scan_accessor)

        expected_resources = [
            Resource(
                resource_id=created_volume_arn,
                type="aws:ec2:volume",
                link_collection=LinkCollection(
                    simple_links=(
                        SimpleLink(pred="availability_zone", obj="us-east-1a"),
                        SimpleLink(pred="create_time", obj=create_time),
                        SimpleLink(pred="size", obj=True),
                        SimpleLink(pred="state", obj="available"),
                        SimpleLink(pred="volume_type", obj="standard"),
                        SimpleLink(pred="encrypted", obj=False),
                    ),
                    resource_links=(
                        ResourceLink(pred="account", obj="arn:aws::::account/123456789012"),
                        ResourceLink(pred="region", obj="arn:aws:::123456789012:region/us-east-1"),
                    ),
                ),
            )
        ]
        self.assertEqual(resources, expected_resources)
