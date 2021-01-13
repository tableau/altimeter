import unittest
import datetime

from dateutil.tz import tzutc
from altimeter.core.graph.links import LinkCollection, ResourceLink, SimpleLink
from altimeter.core.resource.resource import Resource
from altimeter.aws.resource.ec2.vpc_endpoint import VpcEndpointResourceSpec


class TestVpcEndpointResourceSpec(unittest.TestCase):
    def test_schema_parse(self):
        resource_arn = "arn:aws:ec2:us-east-2:111122223333:vpc-endpoint/vpce-01dec98cb17ac542"
        aws_resource_dict = {
            "VpcEndpointId": "vpce-01dec98cb17ac542f",
            "VpcEndpointType": "Gateway",
            "VpcId": "vpc-075db863045facc8e",
            "ServiceName": "com.amazonaws.us-west-2.s3",
            "State": "available",
            "PolicyDocument": '{"Version":"2008-10-17","Statement":[{"Effect":"Allow",'
            '"Principal":"*","Action":"*","Resource":"*"}]}',
            "RouteTableIds": [
                "rtb-0c4c535bc5dee4d4d",
                "rtb-0d107ed1efb1f72a7",
                "rtb-052a8ac1d5ad0ffe6",
            ],
            "SubnetIds": [],
            "Groups": [],
            "PrivateDnsEnabled": False,
            "RequesterManaged": False,
            "NetworkInterfaceIds": [],
            "DnsEntries": [],
            "CreationTimestamp": datetime.datetime(2018, 4, 11, 18, 17, 45, tzinfo=tzutc()),
            "Tags": [],
            "OwnerId": "111122223333",
        }

        link_collection = VpcEndpointResourceSpec.schema.parse(
            data=aws_resource_dict, context={"account_id": "111122223333", "region": "us-west-2"}
        )
        resource = Resource(
            resource_id=resource_arn,
            type=VpcEndpointResourceSpec.type_name,
            link_collection=link_collection,
        )

        expected_resource = Resource(
            resource_id="arn:aws:ec2:us-east-2:111122223333:vpc-endpoint/vpce-01dec98cb17ac542",
            type="vpc-endpoint",
            link_collection=LinkCollection(
                simple_links=(
                    SimpleLink(pred="vpc_endpoint_type", obj="Gateway"),
                    SimpleLink(pred="service_name", obj="com.amazonaws.us-west-2.s3"),
                    SimpleLink(pred="state", obj="available"),
                ),
                resource_links=(
                    ResourceLink(
                        pred="vpc",
                        obj="arn:aws:ec2:us-west-2:111122223333:vpc/vpc-075db863045facc8e",
                    ),
                ),
            ),
        )

        self.assertEqual(resource, expected_resource)
