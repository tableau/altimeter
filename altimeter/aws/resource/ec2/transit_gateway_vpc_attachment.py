"""Resource for Transit Gateway VPC Attachments"""
from typing import Type, TypeVar

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.ec2 import EC2ResourceSpec
from altimeter.aws.resource.ec2.subnet import SubnetResourceSpec
from altimeter.core.graph.schema import Schema
from altimeter.core.graph.field.dict_field import AnonymousDictField
from altimeter.core.graph.field.list_field import AnonymousListField
from altimeter.core.graph.field.resource_link_field import EmbeddedResourceLinkField
from altimeter.core.graph.field.scalar_field import ScalarField

T = TypeVar("T", bound="TransitGatewayVpcAttachmentResourceSpec")


class TransitGatewayVpcAttachmentResourceSpec(EC2ResourceSpec):
    """Resource for Transit Gateway VPC Attachments"""

    type_name = "transit-gateway-vpc-attachment"
    schema = Schema(
        ScalarField("TransitGatewayAttachmentId"),
        ScalarField("TransitGatewayId"),
        ScalarField("VpcId"),
        ScalarField("VpcOwnerId"),
        ScalarField("State"),
        ScalarField("CreationTime"),
        AnonymousListField("SubnetIds", EmbeddedResourceLinkField(SubnetResourceSpec)),
        AnonymousDictField("Options", ScalarField("DnsSupport"), ScalarField("Ipv6Support")),
    )

    @classmethod
    def list_from_aws(
        cls: Type[T], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        paginator = client.get_paginator("describe_transit_gateway_vpc_attachments")
        attachments = {}
        for resp in paginator.paginate():
            for attachment in resp.get("TransitGatewayVpcAttachments", []):
                resource_arn = cls.generate_arn(
                    account_id, region, attachment["TransitGatewayAttachmentId"]
                )
                attachments[resource_arn] = attachment
        return ListFromAWSResult(resources=attachments)
