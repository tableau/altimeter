import json
import tempfile
import unittest
import unittest.mock

import boto3
import moto

from altimeter.aws.auth.accessor import Accessor
from altimeter.aws.auth.cache import AWSCredentialsCache
from altimeter.aws.aws2n import aws2n
from altimeter.aws.resource.iam.policy import IAMPolicyResourceSpec
from altimeter.aws.resource.s3.bucket import S3BucketResourceSpec
from altimeter.aws.resource.ec2.vpc import VPCResourceSpec
from altimeter.aws.scan.muxer.local_muxer import LocalAWSScanMuxer
from altimeter.core.config import AWSConfig, ConcurrencyConfig, ScanConfig


class TestAWS2NSingleAccount(unittest.TestCase):
    @moto.mock_ec2
    @moto.mock_iam
    @moto.mock_s3
    @moto.mock_sts
    def test(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            # TODO FIXME
            # LEFT OFF - I don't think there's a good way to do what we really want to do. OTOH this
            # test would be valuable in the future for any refactoring of core code. let's expand
            # it to as many things as we can and run with it.
            # TODO FIXME

            # get moto's enabled regions
            ec2_client = boto3.client("ec2", region_name="us-east-1")
            regions_resp = ec2_client.describe_regions(
                Filters=[{"Name": "opt-in-status", "Values": ["opt-in-not-required", "opted-in"]}]
            )
            enabled_regions = tuple(region["RegionName"] for region in regions_resp["Regions"])
            # add some resources
            # iam
            iam_client = boto3.client("iam")
            test_policy_document = {
                "Version": "2012-10-17",
                "Statement": [
                    {"Effect": "Allow", "Action": "logs:CreateLogGroup", "Resource": "*"},
                ],
            }
            iam_client.create_policy(
                PolicyName="test_policy", PolicyDocument=json.dumps(test_policy_document),
            )
            # s3
            s3_client = boto3.client("s3")
            s3_client.create_bucket(Bucket="test_bucket")
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
                IAMPolicyResourceSpec,
                S3BucketResourceSpec,
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
