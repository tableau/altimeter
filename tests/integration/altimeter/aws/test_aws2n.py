import datetime
import io
import ipaddress
import json
from pathlib import Path
import tempfile
from typing import Any, Dict, Iterable, Tuple
import unittest
import unittest.mock
import zipfile

import boto3
import moto

from altimeter.aws.auth.accessor import Accessor
from altimeter.aws.auth.cache import AWSCredentialsCache
from altimeter.aws.aws2n import aws2n
from altimeter.aws.resource.awslambda.function import LambdaFunctionResourceSpec
from altimeter.aws.resource.util import policy_doc_dict_to_sorted_str

# from altimeter.aws.resource.dynamodb.dynamodb_table import DynamoDbTableResourceSpec # TODO moto
from altimeter.aws.resource.ec2.flow_log import FlowLogResourceSpec
from altimeter.aws.resource.ec2.image import EC2ImageResourceSpec
from altimeter.aws.resource.ec2.volume import EBSVolumeResourceSpec
from altimeter.aws.resource.ec2.subnet import SubnetResourceSpec
from altimeter.aws.resource.ec2.vpc import VPCResourceSpec
from altimeter.aws.resource.iam.policy import IAMPolicyResourceSpec
from altimeter.aws.resource.iam.role import IAMRoleResourceSpec
from altimeter.aws.resource.s3.bucket import S3BucketResourceSpec
from altimeter.aws.scan.muxer.local_muxer import LocalAWSScanMuxer
from altimeter.core.config import AWSConfig, ConcurrencyConfig, ScanConfig
from altimeter.core.graph.graph_set import GraphSet
from altimeter.core.graph.links import (
    LinkCollection,
    ResourceLink,
    SimpleLink,
    MultiLink,
    TransientResourceLink,
)
from altimeter.core.resource.resource import Resource


class TestAWS2NSingleAccount(unittest.TestCase):
    @moto.mock_dynamodb
    @moto.mock_ec2
    @moto.mock_iam
    @moto.mock_lambda
    @moto.mock_s3
    @moto.mock_sts
    def test(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            resource_region_name = "us-east-1"
            # get moto"s enabled regions
            ec2_client = boto3.client("ec2", region_name=resource_region_name)
            all_regions = ec2_client.describe_regions(
                Filters=[{"Name": "opt-in-status", "Values": ["opt-in-not-required", "opted-in"]}]
            )["Regions"]
            account_id = get_account_id()
            all_region_names = tuple(region["RegionName"] for region in all_regions)
            enabled_region_names = tuple(
                region["RegionName"]
                for region in all_regions
                if region["OptInStatus"] != "not-opted-in"
            )
            delete_vpcs(all_region_names)
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
            bucket_1_name = "test_bucket"
            bucket_1_arn, bucket_1_creation_date = create_bucket(
                name=bucket_1_name, account_id=account_id, region_name=resource_region_name
            )
            ## ec2
            vpc_1_cidr = "10.0.0.0/16"
            vpc_1_id = create_vpc(cidr_block=vpc_1_cidr, region_name=resource_region_name)
            vpc_1_arn = VPCResourceSpec.generate_arn(
                resource_id=vpc_1_id, account_id=account_id, region=resource_region_name
            )
            subnet_1_cidr = "10.0.0.0/24"
            subnet_1_cidr_network = ipaddress.IPv4Network(subnet_1_cidr, strict=False)
            subnet_1_first_ip, subnet_1_last_ip = (
                int(subnet_1_cidr_network[0]),
                int(subnet_1_cidr_network[-1]),
            )
            subnet_1_id = create_subnet(
                cidr_block=subnet_1_cidr, vpc_id=vpc_1_id, region_name=resource_region_name
            )
            subnet_1_arn = SubnetResourceSpec.generate_arn(
                resource_id=subnet_1_id, account_id=account_id, region=resource_region_name
            )
            fixed_bucket_1_arn = f"arn:aws:s3:::{bucket_1_name}"
            flow_log_1_id, flow_log_1_creation_time = create_flow_log(
                vpc_id=vpc_1_id,
                dest_bucket_arn=fixed_bucket_1_arn,
                region_name=resource_region_name,
            )
            flow_log_1_arn = FlowLogResourceSpec.generate_arn(
                resource_id=flow_log_1_id, account_id=account_id, region=resource_region_name
            )
            ebs_volume_1_size = 128
            ebs_volume_1_az = f"{resource_region_name}a"
            ebs_volume_1_arn, ebs_volume_1_create_time = create_volume(
                size=ebs_volume_1_size, az=ebs_volume_1_az, region_name=resource_region_name
            )
            ## iam
            policy_1_name = "test_policy_1"
            policy_1_arn, policy_1_id = create_iam_policy(
                name=policy_1_name,
                policy_doc={
                    "Version": "2012-10-17",
                    "Statement": [
                        {"Effect": "Allow", "Action": "logs:CreateLogGroup", "Resource": "*"},
                    ],
                },
            )
            role_1_name = "test_role_1"
            role_1_assume_role_policy_doc = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Action": "sts:AssumeRole",
                        "Effect": "Allow",
                        "Principal": {"Service": "lambda.amazonaws.com"},
                        "Sid": "",
                    }
                ],
            }
            role_1_description = "Test Role 1"
            role_1_max_session_duration = 3600
            role_1_arn = create_iam_role(
                name=role_1_name,
                assume_role_policy_doc=role_1_assume_role_policy_doc,
                description=role_1_description,
                max_session_duration=role_1_max_session_duration,
            )
            ## lambda
            lambda_function_1_name = "test_lambda_function_1"
            lambda_function_1_runtime = "python3.7"
            lambda_function_1_handler = "lambda_function.lambda_handler"
            lambda_function_1_description = "Test Lambda Function 1"
            lambda_function_1_timeout = 30
            lambda_function_1_memory_size = 256
            lambda_function_1_arn = create_lambda_function(
                name=lambda_function_1_name,
                runtime=lambda_function_1_runtime,
                role_name=role_1_arn,
                handler=lambda_function_1_handler,
                description=lambda_function_1_description,
                timeout=lambda_function_1_timeout,
                memory_size=lambda_function_1_memory_size,
                publish=False,
                region_name=resource_region_name,
            )
            # scan
            test_scan_id = "test_scan_id"
            aws_config = AWSConfig(
                artifact_path=temp_dir,
                pruner_max_age_min=4320,
                graph_name="alti",
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
                FlowLogResourceSpec,
                IAMPolicyResourceSpec,
                IAMRoleResourceSpec,
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
                mock_get_all_enabled_regions.return_value = enabled_region_names
                aws2n_result = aws2n(
                    scan_id=test_scan_id, config=aws_config, muxer=muxer, load_neptune=False,
                )
                graph_set = GraphSet.from_json_file(Path(aws2n_result.json_path))
                self.assertEqual(len(graph_set.errors), 0)
                self.assertEqual(graph_set.name, "alti")
                self.assertEqual(graph_set.version, "2")
                # now check each resource type
                self.maxDiff = None
                ## Accounts
                expected_account_resources = [
                    Resource(
                        resource_id=f"arn:aws::::account/{account_id}",
                        type="aws:account",
                        link_collection=LinkCollection(
                            simple_links=(SimpleLink(pred="account_id", obj=account_id),),
                        ),
                    )
                ]
                account_resources = [
                    resource for resource in graph_set.resources if resource.type == "aws:account"
                ]
                self.assertCountEqual(account_resources, expected_account_resources)
                ## Regions
                expected_region_resources = [
                    Resource(
                        resource_id=f"arn:aws:::{account_id}:region/{region['RegionName']}",
                        type="aws:region",
                        link_collection=LinkCollection(
                            simple_links=(
                                SimpleLink(pred="name", obj=region["RegionName"]),
                                SimpleLink(pred="opt_in_status", obj=region["OptInStatus"]),
                            ),
                            resource_links=(
                                ResourceLink(
                                    pred="account", obj=f"arn:aws::::account/{account_id}"
                                ),
                            ),
                        ),
                    )
                    for region in all_regions
                ]
                region_resources = [
                    resource for resource in graph_set.resources if resource.type == "aws:region"
                ]
                self.assertCountEqual(region_resources, expected_region_resources)
                ## IAM Policies
                expected_iam_policy_resources = [
                    Resource(
                        resource_id=policy_1_arn,
                        type="aws:iam:policy",
                        link_collection=LinkCollection(
                            simple_links=(
                                SimpleLink(pred="name", obj=policy_1_name),
                                SimpleLink(pred="policy_id", obj=policy_1_id),
                                SimpleLink(pred="default_version_id", obj="v1"),
                                SimpleLink(
                                    pred="default_version_policy_document_text",
                                    obj='{"Statement": [{"Action": "logs:CreateLogGroup", "Effect": "Allow", "Resource": "*"}], "Version": "2012-10-17"}',
                                ),
                            ),
                            resource_links=(
                                ResourceLink(
                                    pred="account", obj=f"arn:aws::::account/{account_id}"
                                ),
                            ),
                        ),
                    )
                ]
                iam_policy_resources = [
                    resource
                    for resource in graph_set.resources
                    if resource.type == "aws:iam:policy"
                ]
                self.assertCountEqual(iam_policy_resources, expected_iam_policy_resources)
                ## IAM Roles
                expected_iam_role_resources = [
                    Resource(
                        resource_id=role_1_arn,
                        type="aws:iam:role",
                        link_collection=LinkCollection(
                            simple_links=(
                                SimpleLink(pred="name", obj=role_1_name),
                                SimpleLink(
                                    pred="max_session_duration", obj=role_1_max_session_duration
                                ),
                                SimpleLink(pred="description", obj=role_1_description),
                                SimpleLink(
                                    pred="assume_role_policy_document_text",
                                    obj=policy_doc_dict_to_sorted_str(
                                        role_1_assume_role_policy_doc
                                    ),
                                ),
                            ),
                            multi_links=(
                                MultiLink(
                                    pred="assume_role_policy_document",
                                    obj=LinkCollection(
                                        simple_links=(
                                            SimpleLink(pred="version", obj="2012-10-17"),
                                        ),
                                        multi_links=(
                                            MultiLink(
                                                pred="statement",
                                                obj=LinkCollection(
                                                    simple_links=(
                                                        SimpleLink(pred="effect", obj="Allow"),
                                                        SimpleLink(
                                                            pred="action", obj="sts:AssumeRole"
                                                        ),
                                                    ),
                                                    multi_links=(
                                                        MultiLink(
                                                            pred="principal",
                                                            obj=LinkCollection(
                                                                simple_links=(
                                                                    SimpleLink(
                                                                        pred="service",
                                                                        obj="lambda.amazonaws.com",
                                                                    ),
                                                                )
                                                            ),
                                                        ),
                                                    ),
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                            resource_links=(
                                ResourceLink(pred="account", obj="arn:aws::::account/123456789012"),
                            ),
                        ),
                    )
                ]
                iam_role_resources = [
                    resource for resource in graph_set.resources if resource.type == "aws:iam:role"
                ]
                self.assertCountEqual(iam_role_resources, expected_iam_role_resources)

                ## Lambda functions
                expected_lambda_function_resources = [
                    Resource(
                        resource_id=lambda_function_1_arn,
                        type="aws:lambda:function",
                        link_collection=LinkCollection(
                            simple_links=(
                                SimpleLink(pred="function_name", obj=lambda_function_1_name),
                                SimpleLink(pred="runtime", obj=lambda_function_1_runtime),
                            ),
                            resource_links=(
                                ResourceLink(
                                    pred="account", obj=f"arn:aws::::account/{account_id}"
                                ),
                                ResourceLink(
                                    pred="region",
                                    obj=f"arn:aws:::{account_id}:region/{resource_region_name}",
                                ),
                            ),
                            transient_resource_links=(
                                ResourceLink(
                                    pred="role", obj="arn:aws:iam::123456789012:role/test_role_1"
                                ),
                            ),
                        ),
                    ),
                ]
                lambda_function_resources = [
                    resource
                    for resource in graph_set.resources
                    if resource.type == "aws:lambda:function"
                ]
                self.assertCountEqual(lambda_function_resources, expected_lambda_function_resources)
                ## EC2 VPCs
                expected_ec2_vpc_resources = [
                    Resource(
                        resource_id=vpc_1_arn,
                        type="aws:ec2:vpc",
                        link_collection=LinkCollection(
                            simple_links=(
                                SimpleLink(pred="is_default", obj=False),
                                SimpleLink(pred="cidr_block", obj=vpc_1_cidr),
                                SimpleLink(pred="state", obj="available"),
                            ),
                            resource_links=(
                                ResourceLink(
                                    pred="account", obj=f"arn:aws::::account/{account_id}"
                                ),
                                ResourceLink(
                                    pred="region",
                                    obj=f"arn:aws:::{account_id}:region/{resource_region_name}",
                                ),
                            ),
                        ),
                    )
                ]
                ec2_vpc_resources = [
                    resource for resource in graph_set.resources if resource.type == "aws:ec2:vpc"
                ]
                self.assertCountEqual(ec2_vpc_resources, expected_ec2_vpc_resources)
                ## EC2 VPC Flow Logs
                expected_ec2_vpc_flow_log_resources = [
                    Resource(
                        resource_id=flow_log_1_arn,
                        type="aws:ec2:flow-log",
                        link_collection=LinkCollection(
                            simple_links=(
                                SimpleLink(
                                    pred="creation_time",
                                    obj=flow_log_1_creation_time.replace(
                                        tzinfo=datetime.timezone.utc
                                    ).isoformat(),
                                ),
                                SimpleLink(pred="deliver_logs_status", obj="SUCCESS"),
                                SimpleLink(pred="flow_log_status", obj="ACTIVE"),
                                SimpleLink(pred="traffic_type", obj="ALL"),
                                SimpleLink(pred="log_destination_type", obj="s3"),
                                SimpleLink(pred="log_destination", obj=fixed_bucket_1_arn),
                                SimpleLink(
                                    pred="log_format",
                                    obj="${version} ${account-id} ${interface-id} ${srcaddr} ${dstaddr} ${srcport} ${dstport} ${protocol} ${packets} ${bytes} ${start} ${end} ${action} ${log-status}",
                                ),
                            ),
                            resource_links=(
                                ResourceLink(
                                    pred="account", obj=f"arn:aws::::account/{account_id}"
                                ),
                                ResourceLink(
                                    pred="region",
                                    obj=f"arn:aws:::{account_id}:region/{resource_region_name}",
                                ),
                            ),
                            transient_resource_links=(
                                TransientResourceLink(pred="vpc", obj=vpc_1_arn,),
                            ),
                        ),
                    )
                ]
                ec2_vpc_flow_log_resources = [
                    resource
                    for resource in graph_set.resources
                    if resource.type == "aws:ec2:flow-log"
                ]
                self.assertCountEqual(
                    ec2_vpc_flow_log_resources, expected_ec2_vpc_flow_log_resources
                )
                ## EC2 Subnets
                expected_ec2_subnet_resources = [
                    Resource(
                        resource_id=subnet_1_arn,
                        type="aws:ec2:subnet",
                        link_collection=LinkCollection(
                            simple_links=(
                                SimpleLink(pred="cidr_block", obj=subnet_1_cidr),
                                SimpleLink(pred="first_ip", obj=subnet_1_first_ip),
                                SimpleLink(pred="last_ip", obj=subnet_1_last_ip),
                                SimpleLink(pred="state", obj="available"),
                            ),
                            resource_links=(
                                ResourceLink(pred="vpc", obj=vpc_1_arn),
                                ResourceLink(
                                    pred="account", obj=f"arn:aws::::account/{account_id}"
                                ),
                                ResourceLink(
                                    pred="region",
                                    obj=f"arn:aws:::{account_id}:region/{resource_region_name}",
                                ),
                            ),
                        ),
                    )
                ]
                ec2_subnet_resources = [
                    resource
                    for resource in graph_set.resources
                    if resource.type == "aws:ec2:subnet"
                ]
                self.assertCountEqual(ec2_subnet_resources, expected_ec2_subnet_resources)
                ## EC2 EBS Volumes
                expected_ec2_ebs_volume_resources = [
                    Resource(
                        resource_id=ebs_volume_1_arn,
                        type="aws:ec2:volume",
                        link_collection=LinkCollection(
                            simple_links=(
                                SimpleLink(pred="availability_zone", obj=ebs_volume_1_az),
                                SimpleLink(
                                    pred="create_time",
                                    obj=ebs_volume_1_create_time.replace(
                                        tzinfo=datetime.timezone.utc
                                    ).isoformat(),
                                ),
                                SimpleLink(pred="size", obj=ebs_volume_1_size),
                                SimpleLink(pred="state", obj="available"),
                                SimpleLink(pred="volume_type", obj="gp2"),
                                SimpleLink(pred="encrypted", obj=False),
                            ),
                            resource_links=(
                                ResourceLink(
                                    pred="account", obj=f"arn:aws::::account/{account_id}"
                                ),
                                ResourceLink(
                                    pred="region",
                                    obj=f"arn:aws:::{account_id}:region/{resource_region_name}",
                                ),
                            ),
                        ),
                    )
                ]
                ec2_ebs_volume_resources = [
                    resource
                    for resource in graph_set.resources
                    if resource.type == "aws:ec2:volume"
                ]
                self.assertCountEqual(ec2_ebs_volume_resources, expected_ec2_ebs_volume_resources)
                ## S3 Buckets
                expected_s3_bucket_resources = [
                    Resource(
                        resource_id=bucket_1_arn,
                        type="aws:s3:bucket",
                        link_collection=LinkCollection(
                            simple_links=(
                                SimpleLink(pred="name", obj=bucket_1_name),
                                SimpleLink(
                                    pred="creation_date",
                                    obj=bucket_1_creation_date.replace(
                                        tzinfo=datetime.timezone.utc
                                    ).isoformat(),
                                ),
                            ),
                            resource_links=(
                                ResourceLink(
                                    pred="account", obj=f"arn:aws::::account/{account_id}"
                                ),
                                ResourceLink(
                                    pred="region",
                                    obj=f"arn:aws:::{account_id}:region/{resource_region_name}",
                                ),
                            ),
                        ),
                    )
                ]
                s3_bucket_resources = [
                    resource for resource in graph_set.resources if resource.type == "aws:s3:bucket"
                ]
                self.assertCountEqual(s3_bucket_resources, expected_s3_bucket_resources)

                expected_num_graph_set_resources = (
                    0
                    + len(expected_account_resources)
                    + len(expected_region_resources)
                    + len(expected_iam_policy_resources)
                    + len(expected_iam_role_resources)
                    + len(expected_lambda_function_resources)
                    + len(expected_ec2_ebs_volume_resources)
                    + len(expected_ec2_subnet_resources)
                    + len(expected_ec2_vpc_resources)
                    + len(expected_ec2_vpc_flow_log_resources)
                    + len(expected_s3_bucket_resources)
                )
                self.assertEqual(len(graph_set.resources), expected_num_graph_set_resources)


# helpers


def delete_vpcs(region_names: Iterable[str]) -> None:
    for region_name in region_names:
        regional_ec2_client = boto3.client("ec2", region_name=region_name)
        vpcs_resp = regional_ec2_client.describe_vpcs()
        vpcs = vpcs_resp.get("Vpcs", [])
        for vpc in vpcs:
            vpc_id = vpc["VpcId"]
            subnets_resp = regional_ec2_client.describe_subnets(
                Filters=[{"Name": "vpc-id", "Values": [vpc_id,],},],
            )
            for subnet in subnets_resp["Subnets"]:
                subnet_id = subnet["SubnetId"]
                regional_ec2_client.delete_subnet(SubnetId=subnet_id)
            regional_ec2_client.delete_vpc(VpcId=vpc_id)


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


def create_subnet(cidr_block: str, vpc_id: str, region_name: str) -> str:
    client = boto3.client("ec2", region_name=region_name)
    resp = client.create_subnet(VpcId=vpc_id, CidrBlock=cidr_block,)
    return resp["Subnet"]["SubnetId"]


def create_volume(size: int, az: str, region_name: str) -> Tuple[str, str]:
    client = boto3.client("ec2", region_name=region_name)
    resp = client.create_volume(Size=size, AvailabilityZone=az,)
    volume_id = resp["VolumeId"]
    create_time = resp["CreateTime"]
    account_id = get_account_id()
    return f"arn:aws:ec2:{region_name}:{account_id}:volume/{volume_id}", create_time


def get_account_id() -> str:
    sts_client = boto3.client("sts")
    return sts_client.get_caller_identity()["Account"]


def create_vpc(cidr_block: str, region_name: str) -> str:
    client = boto3.client("ec2", region_name=region_name)
    resp = client.create_vpc(CidrBlock=cidr_block)
    return resp["Vpc"]["VpcId"]


def create_flow_log(vpc_id: str, dest_bucket_arn: str, region_name: str) -> Tuple[str, str]:
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
    flow_log_id = flow_log_ids[0]
    flow_logs_resp = client.describe_flow_logs(FlowLogIds=[flow_log_id])
    creation_time = flow_logs_resp["FlowLogs"][0]["CreationTime"]
    return flow_log_id, creation_time


## iam


def create_iam_policy(name: str, policy_doc: Dict[str, Any]) -> Tuple[str, str]:
    client = boto3.client("iam")
    resp = client.create_policy(PolicyName=name, PolicyDocument=json.dumps(policy_doc),)
    return resp["Policy"]["Arn"], resp["Policy"]["PolicyId"]


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
def create_bucket(name: str, account_id: str, region_name: str) -> Tuple[str, str]:
    client = boto3.client("s3")
    client.create_bucket(Bucket=name)
    buckets = client.list_buckets()["Buckets"]
    creation_date = None
    for bucket in buckets:
        if bucket["Name"] == name:
            creation_date = bucket["CreationDate"]
    if not creation_date:
        raise Exception("BUG: error determining test bucket creation date")
    return f"arn:aws:s3:{region_name}:{account_id}:bucket/{name}", creation_date
