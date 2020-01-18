from unittest import TestCase

import boto3
from moto import mock_ec2, mock_lambda

from altimeter.aws.resource.awslambda.function import LambdaFunctionResourceSpec
from altimeter.aws.scan.aws_accessor import AWSAccessor


class TestLambdaFunctionResourceSpec(TestCase):
    @mock_lambda
    @mock_ec2
    def test_scan(self):
        account_id = "123456789012"
        region_name = "us-east-1"

        session = boto3.Session()

        lambda_client = session.client("lambda", region_name=region_name)
        lambda_client.create_function(
            FunctionName="func_name",
            Runtime="python3.7",
            Role="testrole",
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
            Layers=["test_layer1"],
        )

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        scan_result = LambdaFunctionResourceSpec.scan(scan_accessor=scan_accessor)
        scan_result_dict = scan_result.to_dict()

        self.maxDiff=None
        expected_scan_result_dict = {
            "resources": [
                {
                    "type": "aws:lambda:function",
                    "links": [
                        {"pred": "function_name", "obj": "func_name", "type": "simple"},
                        {
                            "pred": "runtime",
                            "obj": "python3.7",
                            "type": "simple",
                        },
                        {
                            "pred": "vpc",
                            "obj": f"arn:aws:ec2:{region_name}:{account_id}:vpc/vpc-123abc",
                            "type": "transient_resource_link",
                        },
                        {
                            "pred": "account",
                            "obj": f"arn:aws::::account/{account_id}",
                            "type": "resource_link",
                        },
                        {
                            "pred": "region",
                            "obj": f"arn:aws:::{account_id}:region/{region_name}",
                            "type": "resource_link",
                        },
                    ],
                }
            ],
            "errors": [],
        }

        expected_api_call_stats = {
            "count": 1,
            account_id: {
                "count": 1,
                region_name: {
                    "count": 1,
                    "lambda": {"count": 1, "ListFunctions": {"count": 1}},
                },
            },
        }
        self.assertDictEqual(scan_result_dict, expected_scan_result_dict)
        self.assertDictEqual(scan_accessor.api_call_stats.to_dict(), expected_api_call_stats)
