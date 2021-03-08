import io
import json
import tempfile
from typing import Any, Dict, Iterable
import unittest
import unittest.mock
import zipfile

import boto3
import moto

from altimeter.aws.auth.accessor import Accessor
from altimeter.aws.auth.cache import AWSCredentialsCache
from altimeter.aws.aws2n import aws2n
from altimeter.aws.resource.awslambda.function import LambdaFunctionResourceSpec

# TODO moto
# from altimeter.aws.resource.dynamodb.dynamodb_table import DynamoDbTableResourceSpec
from altimeter.aws.resource.ec2.flow_log import FlowLogResourceSpec
from altimeter.aws.resource.ec2.instance import EC2InstanceResourceSpec
from altimeter.aws.resource.ec2.volume import EBSVolumeResourceSpec
from altimeter.aws.resource.ec2.subnet import SubnetResourceSpec
from altimeter.aws.resource.ec2.vpc import VPCResourceSpec
from altimeter.aws.resource.iam.policy import IAMPolicyResourceSpec

# https://github.com/spulec/moto/pull/3750
# from altimeter.aws.resource.iam.role import IAMRoleResourceSpec
from altimeter.aws.resource.s3.bucket import S3BucketResourceSpec
from altimeter.aws.scan.muxer.local_muxer import LocalAWSScanMuxer
from altimeter.core.config import AWSConfig, ConcurrencyConfig, ScanConfig


class TestAWS2NSingleAccount(unittest.TestCase):
    @moto.mock_dynamodb2
    @moto.mock_ec2
    @moto.mock_iam
    @moto.mock_lambda
    @moto.mock_s3
    @moto.mock_sts
    def test(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            # TODO FIXME
            # LEFT OFF - I don't think there's a good way to do what we really want to do. OTOH this
            # test would be valuable in the future for any refactoring of core code. let's expand
            # it to as many things as we can and run with it.
            # TODO FIXME
            region_name = "us-east-1"
            # get moto's enabled regions
            ec2_client = boto3.client("ec2", region_name=region_name)
            regions_resp = ec2_client.describe_regions(
                Filters=[{"Name": "opt-in-status", "Values": ["opt-in-not-required", "opted-in"]}]
            )
            enabled_regions = tuple(region["RegionName"] for region in regions_resp["Regions"])
            delete_vpcs(enabled_regions)
            # add a diverse set of resources which are supported by moto
            ## dynamodb
            # TODO moto is not returning TableId in list/describe
            #            dynamodb_table_1_arn = create_dynamodb_table(
            #                name="test_table_1",
            #                attr_name="test_hash_key_attr_1",
            #                attr_type="S",
            #                key_type="HASH",
            #                region_name=region_name,
            #            )
            ## s3
            bucket_1_arn = create_bucket(name="test_bucket")
            ## ec2
            vpc_1_id = create_vpc(cidr_block="10.0.0.0/16", region_name=region_name)
            subnet_1_id = create_subnet(
                cidr_block="10.0.0.0/24", vpc_id=vpc_1_id, region_name=region_name
            )
            flow_log_1_id = create_flow_log(
                vpc_id=vpc_1_id, dest_bucket_arn=bucket_1_arn, region_name=region_name
            )
            ebs_volume_1_arn = create_volume(
                size=100, az=f"{region_name}a", region_name=region_name
            )
            ami_id = get_ami_id(region_name=region_name)
            instance_1_arn = create_instance(
                ami_id=ami_id, subnet_id=subnet_1_id, region_name=region_name
            )
            # TODO LEFT OFF HERE
            ## iam
            policy_1_arn = create_iam_policy(
                name="test_policy_1",
                policy_doc={
                    "Version": "2012-10-17",
                    "Statement": [
                        {"Effect": "Allow", "Action": "logs:CreateLogGroup", "Resource": "*"},
                    ],
                },
            )
            role_1_arn = create_iam_role(
                name="test_role_1",
                assume_role_policy_doc={},
                description="Test Role 1",
                max_session_duration=3600,
            )
            ## lambda
            lambda_1_arn = create_lambda_function(
                name="test_lambda_function_1",
                runtime="python3.7",
                role_name=role_1_arn,
                handler="lambda_function.lambda_handler",
                description="Test Lambda Function 1",
                timeout=30,
                memory_size=256,
                publish=False,
                region_name=region_name,
            )
            test_scan_id = "test_scan_id"
            aws_config = AWSConfig(
                artifact_path=temp_dir,
                pruner_max_age_min=4320,
                graph_name="alti",
                neptune=None,
                concurrency=ConcurrencyConfig(
                    max_account_scan_threads=1, max_svc_scan_threads=1, max_account_scan_tries=2
                ),
                scan=ScanConfig(
                    accounts=(),
                    regions=(),
                    scan_sub_accounts=False,
                    preferred_account_scan_regions=(
                        "us-west-1",
                        "us-west-2",
                        "us-east-1",
                        "us-east-2",
                    ),
                ),
                accessor=Accessor(
                    credentials_cache=AWSCredentialsCache(cache={}),
                    multi_hop_accessors=[],
                    cache_creds=True,
                ),
                write_master_json=True,
            )
            resource_spec_classes = (
                # DynamoDbTableResourceSpec, TODO moto
                EBSVolumeResourceSpec,
                EC2InstanceResourceSpec,
                FlowLogResourceSpec,
                IAMPolicyResourceSpec,
                # IAMRoleResourceSpec, https://github.com/spulec/moto/pull/3750
                LambdaFunctionResourceSpec,
                S3BucketResourceSpec,
                SubnetResourceSpec,
                VPCResourceSpec,
            )
            muxer = LocalAWSScanMuxer(
                scan_id=test_scan_id,
                config=aws_config,
                resource_spec_classes=resource_spec_classes,
            )
            with unittest.mock.patch(
                "altimeter.aws.scan.account_scanner.get_all_enabled_regions"
            ) as mock_get_all_enabled_regions:
                mock_get_all_enabled_regions.return_value = enabled_regions
                aws2n_result = aws2n(
                    scan_id=test_scan_id, config=aws_config, muxer=muxer, load_neptune=False,
                )
                with open(aws2n_result.json_path, "r") as fp:
                    json_out = json.load(fp)
                    print("*" * 80)
                    print(json.dumps(json_out, indent=2))
                    print("*" * 80)
                raise NotImplementedError


# helpers


def delete_vpcs(enabled_regions: Iterable[str]) -> None:
    for enabled_region_name in enabled_regions:
        regional_ec2_client = boto3.client("ec2", region_name=enabled_region_name)
        vpcs_resp = regional_ec2_client.describe_vpcs()
        vpcs = vpcs_resp.get("Vpcs", [])
        for vpc in vpcs:
            vpc_id = vpc["VpcId"]
            print("**", vpc_id, regional_ec2_client.delete_vpc(VpcId=vpc_id))


## resource builders

## dynamodb


def create_dynamodb_table(
    name: str, attr_name: str, attr_type: str, key_type: str, region_name: str
) -> str:
    client = boto3.client("dynamodb", region_name=region_name)
    resp = client.create_table(
        TableName=name,
        AttributeDefinitions=[{"AttributeName": attr_name, "AttributeType": attr_type,},],
        KeySchema=[{"AttributeName": attr_name, "KeyType": key_type,},],
    )
    return resp["TableDescription"]["TableName"]


## ec2


def create_instance(ami_id: str, subnet_id: str, region_name: str) -> str:
    client = boto3.client("ec2", region_name=region_name)
    resp = client.run_instances(ImageId=ami_id, SubnetId=subnet_id, MinCount=1, MaxCount=1)
    return resp["Instances"][0]["InstanceId"]


def get_ami_id(region_name: str) -> str:
    client = boto3.client("ec2", region_name=region_name)
    resp = client.describe_images()
    return resp["Images"][0]["ImageId"]


def create_subnet(cidr_block: str, vpc_id: str, region_name: str) -> str:
    client = boto3.client("ec2", region_name=region_name)
    resp = client.create_subnet(VpcId=vpc_id, CidrBlock=cidr_block,)
    return resp["Subnet"]["SubnetId"]


def create_volume(size: int, az: str, region_name: str) -> str:
    client = boto3.client("ec2", region_name=region_name)
    resp = client.create_volume(Size=size, AvailabilityZone=az,)
    volume_id = resp["VolumeId"]
    sts_client = boto3.client("sts")
    account_id = sts_client.get_caller_identity()["Account"]
    return f"arn:aws:ec2:{region_name}:{account_id}:volume/{volume_id}"


def create_vpc(cidr_block: str, region_name: str) -> str:
    client = boto3.client("ec2", region_name=region_name)
    resp = client.create_vpc(CidrBlock=cidr_block)
    return resp["Vpc"]["VpcId"]


def create_flow_log(vpc_id: str, dest_bucket_arn: str, region_name: str) -> str:
    client = boto3.client("ec2", region_name=region_name)
    resp = client.create_flow_logs(
        ResourceIds=[vpc_id],
        ResourceType="VPC",
        TrafficType="ALL",
        LogDestinationType="s3",
        LogDestination=dest_bucket_arn,
        MaxAggregationInterval=600,
    )
    flow_log_ids = resp["FlowLogIds"]
    assert len(flow_log_ids) == 1
    return flow_log_ids[0]


## iam


def create_iam_policy(name: str, policy_doc: Dict[str, Any]) -> str:
    client = boto3.client("iam")
    resp = client.create_policy(PolicyName=name, PolicyDocument=json.dumps(policy_doc),)
    return resp["Policy"]["Arn"]


# https://github.com/spulec/moto/pull/3750
def create_iam_role(
    name: str, assume_role_policy_doc: Dict[str, Any], description: str, max_session_duration: int
) -> str:
    client = boto3.client("iam")
    resp = client.create_role(
        RoleName=name,
        AssumeRolePolicyDocument=json.dumps(assume_role_policy_doc),
        Description=description,
        MaxSessionDuration=max_session_duration,
    )
    return resp["Role"]["Arn"]


## lambda


def create_lambda_function(
    name: str,
    runtime: str,
    role_name: str,
    handler: str,
    description: str,
    timeout: int,
    memory_size: int,
    publish: bool,
    region_name: str,
) -> str:
    # create fake zip content
    # create a fake zip BytesIO obj
    zip_output = io.BytesIO()
    zip_file = zipfile.ZipFile(zip_output, "w", zipfile.ZIP_DEFLATED)
    func_str = """
def lambda_handler(event, context):
    print("fake lambda handler")
    return event
    """
    zip_file.writestr("lambda_function.py", func_str)
    zip_file.close()
    zip_output.seek(0)
    zip_content = zip_output.read()
    client = boto3.client("lambda", region_name=region_name)
    resp = client.create_function(
        FunctionName=name,
        Runtime=runtime,
        Role=role_name,
        Handler=handler,
        Code={"ZipFile": zip_content},
        Description=description,
        Timeout=timeout,
        MemorySize=memory_size,
        Publish=publish,
    )
    return resp["FunctionArn"]


## s3
def create_bucket(name: str) -> str:
    client = boto3.client("s3")
    resp = client.create_bucket(Bucket=name)
    return f"arn:aws:s3:::{name}"
