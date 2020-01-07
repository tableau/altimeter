"""Resource for SecurityGroups"""
import ipaddress
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.ec2 import EC2ResourceSpec
from altimeter.core.graph.field.dict_field import EmbeddedDictField
from altimeter.core.graph.field.list_field import ListField
from altimeter.core.graph.field.resource_link_field import ResourceLinkField
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.field.tags_field import TagsField
from altimeter.core.graph.schema import Schema


class SecurityGroupResourceSpec(EC2ResourceSpec):
    """Resource for SecurityGroups"""

    type_name = "security-group"
    schema = Schema(
        ScalarField("GroupName", "name"),
        ListField(
            "IpPermissions",
            EmbeddedDictField(
                ScalarField("IpProtocol"),
                ScalarField("FromPort", default_value=0),
                ScalarField("ToPort", default_value=65535),
                ListField(
                    "IpRanges",
                    EmbeddedDictField(
                        ScalarField("CidrIp"), ScalarField("FirstIp"), ScalarField("LastIp")
                    ),
                    alti_key="ip_range",
                    optional=True,
                ),
                ListField(
                    "Ipv6Ranges",
                    EmbeddedDictField(
                        ScalarField("CidrIpv6"), ScalarField("FirstIp"), ScalarField("LastIp")
                    ),
                    alti_key="ipv6_range",
                    optional=True,
                ),
                ListField(
                    "PrefixListIds", EmbeddedDictField(ScalarField("PrefixListId")), optional=True
                ),
                ListField(
                    "UserIdGroupPairs",
                    EmbeddedDictField(
                        ResourceLinkField("GroupId", "SecurityGroupResourceSpec"),
                        ScalarField("UserId", alti_key="account_id"),
                        ScalarField("PeeringStatus", optional=True),
                        ScalarField("VpcId", optional=True),
                        ScalarField("VpcPeeringConnectionId", optional=True),
                    ),
                    alti_key="user_id_group_pairs",
                ),
            ),
            alti_key="ingress_rule",
        ),
        ListField(
            "IpPermissionsEgress",
            EmbeddedDictField(
                ScalarField("IpProtocol"),
                ScalarField("FromPort", default_value=0),
                ScalarField("ToPort", default_value=65535),
                ListField(
                    "IpRanges",
                    EmbeddedDictField(
                        ScalarField("CidrIp"), ScalarField("FirstIp"), ScalarField("LastIp")
                    ),
                    alti_key="ip_range",
                    optional=True,
                ),
                ListField(
                    "Ipv6Ranges",
                    EmbeddedDictField(
                        ScalarField("CidrIpv6"), ScalarField("FirstIp"), ScalarField("LastIp")
                    ),
                    alti_key="ipv6_range",
                    optional=True,
                ),
                ListField(
                    "PrefixListIds", EmbeddedDictField(ScalarField("PrefixListId")), optional=True
                ),
                ListField(
                    "UserIdGroupPairs",
                    EmbeddedDictField(
                        ResourceLinkField("GroupId", "SecurityGroupResourceSpec"),
                        ScalarField("UserId", alti_key="account_id"),
                        ScalarField("PeeringStatus", optional=True),
                        ScalarField("VpcId", optional=True),
                        ScalarField("VpcPeeringConnectionId", optional=True),
                    ),
                    alti_key="user_id_group_pairs",
                ),
            ),
            alti_key="egress_rule",
        ),
        TagsField(),
    )

    @classmethod
    def list_from_aws(
        cls: Type["SecurityGroupResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'security_group_1_arn': {security_group_1_dict},
             'security_group_2_arn': {security_group_2_dict},
             ...}

        Where the dicts represent results from describe_subnets."""
        security_groups = {}
        paginator = client.get_paginator("describe_security_groups")
        for resp in paginator.paginate():
            for security_group in resp.get("SecurityGroups", []):
                resource_arn = cls.generate_arn(
                    account_id=account_id, region=region, resource_id=security_group["GroupId"]
                )
                for ingress_rule in security_group.get("IpPermissions", []):
                    for ip_range in ingress_rule.get("IpRanges", []):
                        cidr = ip_range["CidrIp"]
                        ipv4_network = ipaddress.IPv4Network(cidr, strict=False)
                        first_ip, last_ip = int(ipv4_network[0]), int(ipv4_network[-1])
                        ip_range["FirstIp"] = first_ip
                        ip_range["LastIp"] = last_ip
                    for ip_range in ingress_rule.get("Ipv6Ranges", []):
                        cidr = ip_range["CidrIpv6"]
                        ipv6_network = ipaddress.IPv6Network(cidr, strict=False)
                        first_ip, last_ip = int(ipv6_network[0]), int(ipv6_network[-1])
                        ip_range["FirstIp"] = first_ip
                        ip_range["LastIp"] = last_ip
                for egress_rule in security_group.get("IpPermissionsEgress", []):
                    for ip_range in egress_rule.get("IpRanges", []):
                        cidr = ip_range["CidrIp"]
                        ipv4_network = ipaddress.IPv4Network(cidr, strict=False)
                        first_ip, last_ip = int(ipv4_network[0]), int(ipv4_network[-1])
                        ip_range["FirstIp"] = first_ip
                        ip_range["LastIp"] = last_ip
                    for ip_range in egress_rule.get("Ipv6Ranges", []):
                        cidr = ip_range["CidrIpv6"]
                        ipv6_network = ipaddress.IPv6Network(cidr, strict=False)
                        first_ip, last_ip = int(ipv6_network[0]), int(ipv6_network[-1])
                        ip_range["FirstIp"] = first_ip
                        ip_range["LastIp"] = last_ip
                security_groups[resource_arn] = security_group
        return ListFromAWSResult(resources=security_groups)
