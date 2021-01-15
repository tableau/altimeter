import boto3
from botocore.exceptions import ClientError
from unittest import TestCase
from moto import mock_ec2, mock_elbv2
from unittest.mock import patch
from altimeter.aws.resource.elbv2.load_balancer import LoadBalancerResourceSpec
from altimeter.aws.scan.aws_accessor import AWSAccessor


class TestLB(TestCase):
    @mock_elbv2
    @mock_ec2
    def test_disappearing_elb_race_condition(self):
        account_id = "123456789012"
        region_name = "us-east-1"
        lb_name = "foo"

        session = boto3.Session()
        ec2_client = session.client("ec2", region_name=region_name)
        moto_subnets = [subnet["SubnetId"] for subnet in ec2_client.describe_subnets()["Subnets"]]

        client = session.client("elbv2", region_name=region_name)

        resp = client.create_load_balancer(Name=lb_name, Subnets=moto_subnets[:2],)
        lb_arn = resp["LoadBalancers"][0]["LoadBalancerArn"]

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        with patch(
            "altimeter.aws.resource.elbv2.load_balancer.LoadBalancerResourceSpec.get_lb_attrs"
        ) as mock_get_lb_attrs:
            mock_get_lb_attrs.side_effect = ClientError(
                operation_name="DescribeLoadBalancerAttributes",
                error_response={
                    "Error": {
                        "Code": "LoadBalancerNotFound",
                        "Message": f"Load balancer '{lb_arn}' not found",
                    }
                },
            )
            resources = LoadBalancerResourceSpec.scan(scan_accessor=scan_accessor)
            self.assertEqual(resources, [])
