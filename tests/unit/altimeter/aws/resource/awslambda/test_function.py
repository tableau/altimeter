import json
from unittest import TestCase

import boto3
from moto import mock_ec2, mock_iam, mock_lambda

from altimeter.aws.resource.awslambda.function import LambdaFunctionResourceSpec
from altimeter.aws.scan.aws_accessor import AWSAccessor
from altimeter.core.graph.links import (
    LinkCollection,
    ResourceLink,
    SimpleLink,
    TransientResourceLink,
)
from altimeter.core.resource.resource import Resource


class TestLambdaFunctionResourceSpec(TestCase):
    @mock_lambda
    @mock_ec2
    @mock_iam
    def test_scan(self):
        account_id = "123456789012"
        region_name = "us-east-1"

        session = boto3.Session()
        iam_client = session.client("iam")
        test_assume_role_policy_doc = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "abc",
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
        iam_role_resp = iam_client.create_role(
            RoleName="testrole", AssumeRolePolicyDocument=json.dumps(test_assume_role_policy_doc),
        )
        iam_role_arn = iam_role_resp["Role"]["Arn"]

        lambda_client = session.client("lambda", region_name=region_name)
        lambda_client.create_function(
            FunctionName="func_name",
            Runtime="python3.7",
            Role=iam_role_arn,
            Handler="testhandler",
            Description="testdescr",
            Timeout=90,
            MemorySize=128,
            Code={"ZipFile": b"1234"},
            Publish=False,
            VpcConfig={"SubnetIds": ["subnet-123"], "SecurityGroupIds": ["sg-123"]},
            DeadLetterConfig={"TargetArn": "test_dl_config"},
            Environment={"Variables": {"TEST_VAR": "test_val"}},
            KMSKeyArn="test_kms_arn",
            TracingConfig={"Mode": "Active"},
            Tags={"tagkey1": "tagval1", "tagkey2": "tagval2"},
        )

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        resources = LambdaFunctionResourceSpec.scan(scan_accessor=scan_accessor)
        expected_resources = [
            Resource(
                resource_id="arn:aws:lambda:us-east-1:123456789012:function:func_name",
                type="aws:lambda:function",
                link_collection=LinkCollection(
                    simple_links=(
                        SimpleLink(pred="function_name", obj="func_name"),
                        SimpleLink(pred="runtime", obj="python3.7"),
                    ),
                    resource_links=(
                        ResourceLink(pred="account", obj="arn:aws::::account/123456789012"),
                        ResourceLink(pred="region", obj="arn:aws:::123456789012:region/us-east-1"),
                    ),
                    transient_resource_links=(
                        TransientResourceLink(
                            pred="vpc", obj="arn:aws:ec2:us-east-1:123456789012:vpc/vpc-123abc"
                        ),
                        TransientResourceLink(
                            pred="role", obj="arn:aws:iam::123456789012:role/testrole"
                        ),
                    ),
                ),
            )
        ]
        self.maxDiff = None
        self.assertEqual(resources, expected_resources)
