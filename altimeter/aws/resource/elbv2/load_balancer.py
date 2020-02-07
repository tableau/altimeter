"""Resource for load balancers"""
from typing import Dict, Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.elbv2 import ELBV2ResourceSpec
from altimeter.aws.resource.ec2.security_group import SecurityGroupResourceSpec
from altimeter.aws.resource.ec2.vpc import VPCResourceSpec
from altimeter.aws.resource.ec2.subnet import SubnetResourceSpec
from altimeter.aws.resource.s3.bucket import S3BucketResourceSpec
from altimeter.core.graph.field.dict_field import AnonymousDictField, EmbeddedDictField
from altimeter.core.graph.field.list_field import ListField
from altimeter.core.graph.field.resource_link_field import (
    EmbeddedResourceLinkField,
    ResourceLinkField,
    TransientResourceLinkField,
)
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class LoadBalancerResourceSpec(ELBV2ResourceSpec):
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
        ScalarField("AccessLogsEnabled"),
        TransientResourceLinkField(
            "AccessLogsS3Bucket",
            S3BucketResourceSpec,
            alti_key="access_logs_s3_bucket",
            optional=True,
        ),
        ScalarField("AccessLogsS3Prefix", optional=True),
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
                lb_attrs = cls.get_lb_attrs(client, resource_arn)
                lb.update(lb_attrs)
                load_balancers[resource_arn] = lb
        return ListFromAWSResult(resources=load_balancers)

    @classmethod
    def get_lb_attrs(
        cls: Type["LoadBalancerResourceSpec"], client: BaseClient, lb_arn: str,
    ) -> Dict[str, str]:
        """Get lb attributes that Altimeter graphs."""
        lb_attrs = {}
        resp = client.describe_load_balancer_attributes(LoadBalancerArn=lb_arn)
        for attr in resp["Attributes"]:
            if attr["Key"] == "access_logs.s3.enabled":
                lb_attrs["AccessLogsEnabled"] = attr["Value"]
            elif attr["Key"] == "access_logs.s3.bucket":
                if attr["Value"]:
                    lb_attrs["AccessLogsS3Bucket"] = attr["Value"]
            elif attr["Key"] == "access_logs.s3.prefix":
                if attr["Value"]:
                    lb_attrs["AccessLogsS3Prefix"] = attr["Value"]
        return lb_attrs
