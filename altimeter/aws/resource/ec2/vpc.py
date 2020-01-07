"""Resource for VPCs"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.ec2 import EC2ResourceSpec
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.field.tags_field import TagsField
from altimeter.core.graph.schema import Schema


class VPCResourceSpec(EC2ResourceSpec):
    """Resource for VPCs"""

    type_name = "vpc"
    schema = Schema(
        ScalarField("IsDefault"), ScalarField("CidrBlock"), ScalarField("State"), TagsField()
    )

    @classmethod
    def list_from_aws(
        cls: Type["VPCResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'vpc_1_arn': {vpc_1_dict},
             'vpc_2_arn': {vpc_2_dict},
             ...}

        Where the dicts represent results from describe_vpcs."""
        vpcs = {}
        paginator = client.get_paginator("describe_vpcs")
        for resp in paginator.paginate():
            for vpc in resp.get("Vpcs", []):
                resource_arn = cls.generate_arn(
                    account_id=account_id, region=region, resource_id=vpc["VpcId"]
                )
                vpcs[resource_arn] = vpc
        return ListFromAWSResult(resources=vpcs)
