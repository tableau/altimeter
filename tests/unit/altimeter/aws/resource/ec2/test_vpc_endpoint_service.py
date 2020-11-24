import unittest

from altimeter.core.resource.resource import Resource
from altimeter.aws.resource.ec2.vpc_endpoint_service import VpcEndpointServiceResourceSpec


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

        links = VpcEndpointServiceResourceSpec.schema.parse(
            data=aws_resource_dict, context={"account_id": "111122223333", "region": "us-west-2"}
        )
        resource = Resource(
            resource_id=resource_arn,
            type_name=VpcEndpointServiceResourceSpec.type_name,
            links=links,
        )
        alti_resource_dict = resource.to_dict()

        expected_alti_resource_dict = {
            "type": "vpc-endpoint-service",
            "links": [
                {"pred": "service_type", "obj": "Interface", "type": "simple"},
                {
                    "pred": "service_name",
                    "obj": "com.amazonaws.vpce.us-west-2.vpce-svc-01234abcd5678ef01",
                    "type": "simple",
                },
                {"pred": "service_state", "obj": "Available", "type": "simple"},
                {"pred": "acceptance_required", "obj": True, "type": "simple"},
                {"pred": "availability_zones", "obj": "us-west-2a", "type": "simple"},
                {"pred": "availability_zones", "obj": "us-west-2b", "type": "simple"},
                {"pred": "Name", "obj": "Splunk HEC", "type": "tag"},
            ],
        }
        self.assertDictEqual(alti_resource_dict, expected_alti_resource_dict)
