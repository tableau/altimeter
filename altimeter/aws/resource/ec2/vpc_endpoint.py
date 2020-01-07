"""Resource for VPC Endpoints"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.ec2 import EC2ResourceSpec
from altimeter.aws.resource.ec2.vpc import VPCResourceSpec
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.field.resource_link_field import ResourceLinkField
from altimeter.core.graph.field.tags_field import TagsField
from altimeter.core.graph.schema import Schema


class VpcEndpointResourceSpec(EC2ResourceSpec):
    """Resource for VPC Endpoints"""

    type_name = "vpc-endpoint"
    schema = Schema(
        ScalarField("VpcEndpointType"),
        ScalarField("ServiceName"),
        ScalarField("State"),
        ResourceLinkField("VpcId", VPCResourceSpec),
        TagsField(),
    )

    @classmethod
    def list_from_aws(
        cls: Type["VpcEndpointResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'vpc_endpoint_1_arn': {vpc_endpoint_1_dict},
             'vpc_endpoint_1_arn': {vpc_endpoint_2_dict},
             ...}

        Where the dicts represent results from describe_vpc_endpoints."""
        endpoints = {}
        paginator = client.get_paginator("describe_vpc_endpoints")
        for resp in paginator.paginate():
            for endpoint in resp.get("VpcEndpoints", []):
                resource_arn = cls.generate_arn(
                    account_id=account_id, region=region, resource_id=endpoint["VpcEndpointId"]
                )
                endpoints[resource_arn] = endpoint
        return ListFromAWSResult(resources=endpoints)
