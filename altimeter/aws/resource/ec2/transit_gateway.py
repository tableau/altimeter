"""Resource for Transit Gateways"""
from typing import Any, Dict, List, Type

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


class TransitGatewayResourceSpec(EC2ResourceSpec):
    """Resource for TransitGateways"""

    type_name = "transit-gateway"
    schema = Schema(
        ScalarField("OwnerId"),
        ScalarField("State"),
        ListField(
            "VPCAttachments",
            EmbeddedDictField(
                ScalarField("CreationTime"),
                ScalarField("State"),
                ResourceLinkField("ResourceId", VPCResourceSpec),
            ),
            optional=True,
            alti_key="vpc_attachment",
        ),
        TagsField(),
    )

    @classmethod
    def list_from_aws(
        cls: Type["TransitGatewayResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'tgw_1_arn': {tgw_1_dict},
             'tgw_2_arn': {tgw_2_dict},
             ...}

        Where the dicts represent results from describe_transit_gateways."""
        tgws = {}
        paginator = client.get_paginator("describe_transit_gateways")
        tgw_filters = [{"Name": "owner-id", "Values": [account_id]}]
        for resp in paginator.paginate(Filters=tgw_filters):
            for tgw in resp["TransitGateways"]:
                resource_arn = tgw["TransitGatewayArn"]
                vpc_attachments: List[Dict[str, Any]] = []
                vpc_attachments_paginator = client.get_paginator(
                    "describe_transit_gateway_attachments"
                )
                vpc_filters = [
                    {"Name": "transit-gateway-id", "Values": [tgw["TransitGatewayId"]]},
                    {"Name": "resource-type", "Values": ["vpc"]},
                ]
                for vpc_attachments_resp in vpc_attachments_paginator.paginate(Filters=vpc_filters):
                    vpc_attachments += vpc_attachments_resp["TransitGatewayAttachments"]
                tgw["VPCAttachments"] = vpc_attachments
                tgws[resource_arn] = tgw
        return ListFromAWSResult(resources=tgws)
