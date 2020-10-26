"""Resource for AWS CloudTrail Trails"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.cloudtrail import CloudTrailResourceSpec
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class CloudTrailTrailResourceSpec(CloudTrailResourceSpec):
    """Resource representing a CloudTrail Trail"""

    type_name = "trail"
    schema = Schema(
        ScalarField("Name"),
        ScalarField("S3BucketName"),
        ScalarField("IncludeGlobalServiceEvents"),
        ScalarField("IsMultiRegionTrail"),
    )

    @classmethod
    def list_from_aws(
        cls: Type["CloudTrailTrailResourceSpec"], client: BaseClient, account_id: str, region: str,
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'trail_1_arn': {trail_1_dict},
             'trail_2_arn': {trail_2_dict},
             ...}

        Where the dicts represent results from describe_trails."""
        trails = {}
        resp = client.describe_trails(includeShadowTrails=False)
        for trail in resp.get("trailList", []):
            resource_arn = trail["TrailARN"]
            trails[resource_arn] = trail
        return ListFromAWSResult(resources=trails)
