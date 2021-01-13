import unittest

from altimeter.aws.resource.ec2.vpc_endpoint_service import VpcEndpointServiceResourceSpec
from altimeter.core.resource.resource import Resource
from altimeter.core.graph.links import LinkCollection, SimpleLink, TagLink


class TestVpcEndpointServiceResourceSpec(unittest.TestCase):
    maxDiff = None

    def test_schema_parse(self):
        resource_arn = "arn:aws:ec2:us-west-2:111122223333:vpc-endpoint-service/com.amazonaws.vpce.us-west-2.vpce-svc-01234abcd5678ef01"
        aws_resource_dict = {
            "ServiceType": [{"ServiceType": "Interface"}],
            "ServiceId": "vpce-svc-01234abcd5678ef01",
            "ServiceName": "com.amazonaws.vpce.us-west-2.vpce-svc-01234abcd5678ef01",
            "ServiceState": "Available",
            "AvailabilityZones": ["us-west-2a", "us-west-2b"],
            "AcceptanceRequired": True,
            "ManagesVpcEndpoints": False,
            "NetworkLoadBalancerArns": [
                "arn:aws:elasticloadbalancing:us-west-2:111122223333:loadbalancer/net/splunk-hwf-lb/1a7ff9c18eeaaf9b"
            ],
            "BaseEndpointDnsNames": ["vpce-svc-01234abcd5678ef01.us-west-2.vpce.amazonaws.com"],
            "PrivateDnsNameConfiguration": {},
            "Tags": [{"Key": "Name", "Value": "Splunk HEC"}],
        }

        link_collection = VpcEndpointServiceResourceSpec.schema.parse(
            data=aws_resource_dict, context={"account_id": "111122223333", "region": "us-west-2"}
        )
        resource = Resource(
            resource_id=resource_arn,
            type=VpcEndpointServiceResourceSpec.type_name,
            link_collection=link_collection,
        )

        expected_resource = Resource(
            resource_id="arn:aws:ec2:us-west-2:111122223333:vpc-endpoint-service/com.amazonaws.vpce.us-west-2.vpce-svc-01234abcd5678ef01",
            type="vpc-endpoint-service",
            link_collection=LinkCollection(
                simple_links=(
                    SimpleLink(pred="service_type", obj="Interface"),
                    SimpleLink(
                        pred="service_name",
                        obj="com.amazonaws.vpce.us-west-2.vpce-svc-01234abcd5678ef01",
                    ),
                    SimpleLink(pred="service_state", obj="Available"),
                    SimpleLink(pred="acceptance_required", obj=True),
                    SimpleLink(pred="availability_zones", obj="us-west-2a"),
                    SimpleLink(pred="availability_zones", obj="us-west-2b"),
                ),
                tag_links=(TagLink(pred="Name", obj="Splunk HEC"),),
            ),
        )
        self.assertEqual(resource, expected_resource)
