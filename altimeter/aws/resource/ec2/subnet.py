"""Resource for Subnets"""
import ipaddress
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.ec2 import EC2ResourceSpec
from altimeter.aws.resource.ec2.vpc import VPCResourceSpec
from altimeter.core.graph.field.resource_link_field import ResourceLinkField
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.field.tags_field import TagsField
from altimeter.core.graph.schema import Schema


class SubnetResourceSpec(EC2ResourceSpec):
    """Resource for Subnets"""

    type_name = "subnet"
    schema = Schema(
        ScalarField("CidrBlock"),
        ScalarField("FirstIp"),
        ScalarField("LastIp"),
        ScalarField("State"),
        TagsField(),
        ResourceLinkField("VpcId", VPCResourceSpec),
    )

    @classmethod
    def list_from_aws(
        cls: Type["SubnetResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        subnets = {}
        resp = client.describe_subnets()
        for subnet in resp.get("Subnets", []):
            resource_arn = cls.generate_arn(
                account_id=account_id, region=region, resource_id=subnet["SubnetId"]
            )
            cidr = subnet["CidrBlock"]
            ipv4_network = ipaddress.IPv4Network(cidr, strict=False)
            first_ip, last_ip = int(ipv4_network[0]), int(ipv4_network[-1])
            subnet["FirstIp"] = first_ip
            subnet["LastIp"] = last_ip
            subnets[resource_arn] = subnet
        return ListFromAWSResult(resources=subnets)
