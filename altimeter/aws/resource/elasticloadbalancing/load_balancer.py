"""Resource for load balancers"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.elasticloadbalancing import ElasticLoadBalancingResourceSpec
from altimeter.aws.resource.ec2.security_group import SecurityGroupResourceSpec
from altimeter.aws.resource.ec2.vpc import VPCResourceSpec
from altimeter.aws.resource.ec2.subnet import SubnetResourceSpec
from altimeter.core.graph.field.dict_field import AnonymousDictField, EmbeddedDictField
from altimeter.core.graph.field.list_field import ListField
from altimeter.core.graph.field.resource_link_field import (
    EmbeddedResourceLinkField,
    ResourceLinkField,
)
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class LoadBalancerResourceSpec(ElasticLoadBalancingResourceSpec):
    """Resource for load balancer"""

    type_name = "loadbalancer"
    schema = Schema(
        ScalarField("DNSName"),
        ScalarField("CreatedTime"),
        ScalarField("LoadBalancerName"),
        ScalarField("Scheme"),
        ResourceLinkField("VpcId", VPCResourceSpec, optional=True),
        AnonymousDictField("State", ScalarField("Code", alti_key="load_balancer_state")),
        ScalarField("Type"),
        ListField(
            "AvailabilityZones",
            EmbeddedDictField(
                ScalarField("ZoneName"),
                ResourceLinkField("SubnetId", SubnetResourceSpec, optional=True),
                ListField(
                    "LoadBalancerAddresses",
                    EmbeddedDictField(
                        ScalarField("IpAddress", optional=True),
                        ScalarField("AllocationId", optional=True),
                    ),
                    optional=True,
                ),
            ),
        ),
        ListField(
            "SecurityGroups", EmbeddedResourceLinkField(SecurityGroupResourceSpec), optional=True
        ),
        ScalarField("IpAddressType"),
    )

    @classmethod
    def list_from_aws(
        cls: Type["LoadBalancerResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'lb_1_arn': {lb_1_dict},
             'lb_2_arn': {lb_2_dict},
             ...}

        Where the dicts represent results from describe_load_balancers."""
        paginator = client.get_paginator("describe_load_balancers")
        load_balancers = {}
        for resp in paginator.paginate():
            for lb in resp.get("LoadBalancers", []):
                resource_arn = lb["LoadBalancerArn"]
                load_balancers[resource_arn] = lb
        return ListFromAWSResult(resources=load_balancers)
