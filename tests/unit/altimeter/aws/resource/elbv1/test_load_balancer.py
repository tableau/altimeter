import boto3
from botocore.exceptions import ClientError
from unittest import TestCase
from moto import mock_elb
from unittest.mock import patch
from altimeter.aws.resource.elbv1.load_balancer import ClassicLoadBalancerResourceSpec
from altimeter.aws.scan.aws_accessor import AWSAccessor


class TestLB(TestCase):
    @mock_elb
    def test_disappearing_elb_race_condition(self):
        account_id = "123456789012"
        region_name = "us-east-1"
        lb_name = "foo"

        session = boto3.Session()
        client = session.client("elb", region_name=region_name)

        client.create_load_balancer(
            LoadBalancerName=lb_name,
            Listeners=[{"Protocol": "HTTP", "LoadBalancerPort": 80, "InstancePort": 80}],
            Tags=[{"Key": "Name", "Value": lb_name}],
        )

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        with patch(
            "altimeter.aws.resource.elbv1.load_balancer.ClassicLoadBalancerResourceSpec.get_lb_attrs"
        ) as mock_get_lb_attrs:
            mock_get_lb_attrs.side_effect = ClientError(
                operation_name="DescribeLoadBalancerAttributes",
                error_response={
                    "Error": {
                        "Code": "LoadBalancerNotFound",
                        "Message": f"There is no ACTIVE Load Balancer named '{lb_name}'",
                    }
                },
            )
            resources = ClassicLoadBalancerResourceSpec.scan(scan_accessor=scan_accessor)
            self.assertEqual(resources, [])
