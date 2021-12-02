"""Resource for HostedZones"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.route53 import Route53ResourceSpec
from altimeter.core.graph.field.dict_field import EmbeddedDictField
from altimeter.core.graph.field.list_field import ListField
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class HostedZoneResourceSpec(Route53ResourceSpec):
    """Resource for S3 Buckets"""

    type_name = "hostedzone"
    schema = Schema(
        ScalarField("Name"),
        ListField(
            "ResourceRecordSets",
            EmbeddedDictField(
                ScalarField("Name"), ScalarField("Type"), ScalarField("TTL", optional=True)
            ),
            optional=True,
            alti_key="resource_record_set",
        ),
    )

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
                record_sets_paginator = client.get_paginator("list_resource_record_sets")
                zone_resource_record_sets = []
                for record_sets_resp in record_sets_paginator.paginate(HostedZoneId=hosted_zone_id):
                    zone_resource_record_sets += record_sets_resp.get("ResourceRecordSets", [])
                hosted_zone["ResourceRecordSets"] = zone_resource_record_sets
                hosted_zones[resource_arn] = hosted_zone
        return ListFromAWSResult(resources=hosted_zones)
