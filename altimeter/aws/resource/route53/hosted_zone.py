"""Resource for HostedZones"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.route53 import Route53ResourceSpec
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class HostedZoneResourceSpec(Route53ResourceSpec):
    """Resource for S3 Buckets"""

    type_name = "hostedzone"
    schema = Schema(ScalarField("Name"),)

    @classmethod
    def list_from_aws(
        cls: Type["HostedZoneResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'hosted_zone_1_arn': {hosted_zone_1_dict},
             'hosted_zone_2_arn': {hosted_zone_2_dict},
             ...}

        Where the dicts represent results from list_hosted_zones."""
        hosted_zones = {}
        paginator = client.get_paginator("list_hosted_zones")
        for resp in paginator.paginate():
            for hosted_zone in resp.get("HostedZones", []):
                hosted_zone_id = hosted_zone["Id"].split("/")[-1]
                resource_arn = cls.generate_arn(resource_id=hosted_zone_id, account_id=account_id)
                hosted_zones[resource_arn] = hosted_zone
        return ListFromAWSResult(resources=hosted_zones)
