"""Resource for EC2 Network Interfaces."""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.ec2 import EC2ResourceSpec
from altimeter.aws.resource.ec2.vpc import VPCResourceSpec
from altimeter.aws.resource.ec2.subnet import SubnetResourceSpec
from altimeter.core.graph.field.dict_field import AnonymousDictField
from altimeter.core.graph.field.resource_link_field import ResourceLinkField
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class EC2NetworkInterfaceResourceSpec(EC2ResourceSpec):
    """Resource for EC2NetworkInterfaces"""

    type_name = "network-interface"
    schema = Schema(
        AnonymousDictField(
            "Association",
            ScalarField("PublicDnsName", optional=True),
            ScalarField("PublicIp", optional=True),
            optional=True,
        ),
        ScalarField("Description"),
        ScalarField("InterfaceType"),
        ScalarField("MacAddress"),
        ScalarField("PrivateDnsName", optional=True),
        ScalarField("PrivateIpAddress", optional=True),
        ScalarField("Status"),
        ResourceLinkField("SubnetId", SubnetResourceSpec, optional=True),
        ResourceLinkField("VpcId", VPCResourceSpec, optional=True),
    )

    @classmethod
    def list_from_aws(
        cls: Type["EC2NetworkInterfaceResourceSpec"],
        client: BaseClient,
        account_id: str,
        region: str,
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'network_interface_1_arn': {network_interface_1_dict},
             'network_interface_2_arn': {network_interface_2_dict},
             ...}

        Where the dicts represent results from describe_network_interfaces."""
        paginator = client.get_paginator("describe_network_interfaces")
        interfaces = {}
        for resp in paginator.paginate():
            for interface in resp.get("NetworkInterfaces", []):
                resource_arn = cls.generate_arn(
                    account_id=account_id,
                    region=region,
                    resource_id=interface["NetworkInterfaceId"],
                )
                interfaces[resource_arn] = interface
        return ListFromAWSResult(resources=interfaces)
