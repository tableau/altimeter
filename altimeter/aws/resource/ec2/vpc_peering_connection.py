"""Resource for VPC Peering Connections."""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.ec2 import EC2ResourceSpec
from altimeter.aws.resource.ec2.vpc import VPCResourceSpec
from altimeter.core.graph.field.resource_link_field import TransientResourceLinkField
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.field.tags_field import TagsField
from altimeter.core.graph.schema import Schema


class VPCPeeringConnectionResourceSpec(EC2ResourceSpec):
    """Resource for VPC Peering Connections."""

    type_name = "vpc-peering-connection"
    schema = Schema(
        TransientResourceLinkField(
            "AccepterVpc", VPCResourceSpec, value_is_id=True, alti_key="accepter_vpc"
        ),
        TransientResourceLinkField(
            "RequesterVpc", VPCResourceSpec, value_is_id=True, alti_key="requester_vpc"
        ),
        ScalarField("Status"),
        TagsField(),
    )

    @classmethod
    def list_from_aws(
        cls: Type["VPCPeeringConnectionResourceSpec"],
        client: BaseClient,
        account_id: str,
        region: str,
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'vpc_peering_connection_1_arn': {vpc_peering_connection_1_dict},
             'vpc_peering_connection_2_arn': {vpc_peering_connection_2_dict},
             ...}

        Where the dicts represent results from describe_vpc_peering_connections."""
        vpc_peering_connections = {}
        paginator = client.get_paginator("describe_vpc_peering_connections")
        for resp in paginator.paginate():
            for vpc_pc in resp["VpcPeeringConnections"]:
                resource_arn = cls.generate_arn(
                    resource_id=vpc_pc["VpcPeeringConnectionId"],
                    account_id=account_id,
                    region=region,
                )
                accepter_info = vpc_pc["AccepterVpcInfo"]
                accepter_account_id = accepter_info["OwnerId"]
                accepter_vpc_id = accepter_info["VpcId"]
                accepter_region = accepter_info["Region"]
                accepter_vpc_arn = VPCResourceSpec.generate_arn(
                    resource_id=accepter_vpc_id,
                    account_id=accepter_account_id,
                    region=accepter_region,
                )
                requester_info = vpc_pc["RequesterVpcInfo"]
                requester_account_id = requester_info["OwnerId"]
                requester_vpc_id = requester_info["VpcId"]
                requester_region = requester_info["Region"]
                requester_vpc_arn = VPCResourceSpec.generate_arn(
                    resource_id=requester_vpc_id,
                    account_id=requester_account_id,
                    region=requester_region,
                )
                vpc_peering_connection = {
                    "AccepterVpc": accepter_vpc_arn,
                    "RequesterVpc": requester_vpc_arn,
                    "Status": vpc_pc["Status"]["Code"],
                }
                vpc_peering_connections[resource_arn] = vpc_peering_connection
        return ListFromAWSResult(resources=vpc_peering_connections)
