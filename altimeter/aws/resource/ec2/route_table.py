"""Resource for Route Tables"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.ec2 import EC2ResourceSpec
from altimeter.aws.resource.ec2.vpc import VPCResourceSpec
from altimeter.core.graph.schema import Schema
from altimeter.core.graph.field.dict_field import EmbeddedDictField
from altimeter.core.graph.field.list_field import AnonymousListField, ListField
from altimeter.core.graph.field.resource_link_field import ResourceLinkField
from altimeter.core.graph.field.scalar_field import ScalarField


class EC2RouteTableResourceSpec(EC2ResourceSpec):
    """Resource for Route Tables"""

    type_name = "route-table"

    schema = Schema(
        ScalarField("RouteTableId"),
        ResourceLinkField("VpcId", VPCResourceSpec),
        ScalarField("OwnerId", optional=True),
        AnonymousListField("PropagatingVgws", ScalarField("GatewayId"), optional=True),
        ListField(
            "Routes",
            EmbeddedDictField(
                ScalarField("DestinationCidrBlock", optional=True),
                ScalarField("DestinationIpv6CidrBlock", optional=True),
                ScalarField("DestinationPrefixListId", optional=True),
                ScalarField("EgressOnlyInternetGatewayId", optional=True),
                ScalarField("GatewayId", optional=True),
                ScalarField("InstanceId", optional=True),
                ScalarField("InstanceOwnerId", optional=True),
                ScalarField("NatGatewayId", optional=True),
                ScalarField("TransitGatewayId", optional=True),
                ScalarField("NetworkInterfaceId", optional=True),
                ScalarField("Origin", optional=True),
                ScalarField("State", optional=True),
                ScalarField("VpcPeeringConnectionId", optional=True),
            ),
            optional=True,
            alti_key="route",
        ),
        ListField(
            "Associations",
            EmbeddedDictField(
                ScalarField("Main", optional=True),
                ScalarField("RouteTableAssociationId", optional=True),
                ScalarField("RouteTableId", optional=True),
                ScalarField("SubnetId", optional=True),
            ),
            optional=True,
            alti_key="association",
        ),
    )

    @classmethod
    def list_from_aws(
        cls: Type["EC2RouteTableResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        paginator = client.get_paginator("describe_route_tables")
        route_tables = {}
        for resp in paginator.paginate():
            for attachment in resp.get("RouteTables", []):
                resource_arn = cls.generate_arn(
                    account_id=account_id, region=region, resource_id=attachment["RouteTableId"]
                )
                route_tables[resource_arn] = attachment
        return ListFromAWSResult(resources=route_tables)
