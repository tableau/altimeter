"""Resource for Internet Gateways"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.ec2 import EC2ResourceSpec
from altimeter.aws.resource.ec2.vpc import VPCResourceSpec
from altimeter.core.graph.field.dict_field import EmbeddedDictField
from altimeter.core.graph.field.list_field import ListField
from altimeter.core.graph.field.resource_link_field import ResourceLinkField
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.field.tags_field import TagsField
from altimeter.core.graph.schema import Schema


class InternetGatewayResourceSpec(EC2ResourceSpec):
    """Resource for InternetGateways"""

    type_name = "internet-gateway"
    schema = Schema(
        ScalarField("OwnerId"),
        ListField(
            "Attachments",
            EmbeddedDictField(ScalarField("State"), ResourceLinkField("VpcId", VPCResourceSpec),),
            optional=True,
            alti_key="attachment",
        ),
        TagsField(),
    )

    @classmethod
    def list_from_aws(
        cls: Type["InternetGatewayResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'igw_1_arn': {igw_1_dict},
             'igw_2_arn': {igw_2_dict},
             ...}

        Where the dicts represent results from describe_internet_gateways."""
        igws = {}
        paginator = client.get_paginator("describe_internet_gateways")
        for resp in paginator.paginate():
            for igw in resp["InternetGateways"]:
                resource_arn = cls.generate_arn(
                    resource_id=igw["InternetGatewayId"], account_id=account_id, region=region
                )
                igws[resource_arn] = igw
        return ListFromAWSResult(resources=igws)
