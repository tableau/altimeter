"""Resource for classic load balancers"""
from typing import Dict, Type

from botocore.client import BaseClient

from altimeter.aws.resource.ec2.security_group import SecurityGroupResourceSpec
from altimeter.aws.resource.ec2.subnet import SubnetResourceSpec
from altimeter.aws.resource.ec2.vpc import VPCResourceSpec
from altimeter.aws.resource.elbv1 import ELBV1ResourceSpec
from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.s3.bucket import S3BucketResourceSpec

from altimeter.core.graph.field.resource_link_field import (
    EmbeddedResourceLinkField,
    ResourceLinkField,
    TransientResourceLinkField,
)
from altimeter.core.graph.field.list_field import ListField
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class ClassicLoadBalancerResourceSpec(ELBV1ResourceSpec):
    """Resource for classic load balancer"""

    type_name = "loadbalancer"
    schema = Schema(
        ScalarField("DNSName"),
        ScalarField("CreatedTime"),
        ScalarField("LoadBalancerName"),
        ScalarField("Scheme"),
        ResourceLinkField("VPCId", VPCResourceSpec, optional=True),
        ListField("Subnets", EmbeddedResourceLinkField(SubnetResourceSpec), optional=True),
        ListField(
            "SecurityGroups", EmbeddedResourceLinkField(SecurityGroupResourceSpec), optional=True
        ),
        ScalarField("Type"),
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
        cls: Type["ClassicLoadBalancerResourceSpec"],
        client: BaseClient,
        account_id: str,
        region: str,
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'lb_1_arn': {lb_1_dict},
             'lb_2_arn': {lb_2_dict},
             ...}

        Where the dicts represent results from describe_load_balancers."""
        paginator = client.get_paginator("describe_load_balancers")
        load_balancers = {}
        for resp in paginator.paginate():
            for lb in resp["LoadBalancerDescriptions"]:
                lb_name = lb["LoadBalancerName"]
                resource_arn = cls.generate_arn(
                    account_id=account_id, region=region, resource_id=lb_name
                )
                lb_attrs = cls.get_lb_attrs(client, lb_name)
                lb.update(lb_attrs)
                lb["Type"] = "classic"
                load_balancers[resource_arn] = lb
        return ListFromAWSResult(resources=load_balancers)

    @classmethod
    def get_lb_attrs(
        cls: Type["ClassicLoadBalancerResourceSpec"], client: BaseClient, lb_name: str,
    ) -> Dict[str, str]:
        """Get lb attributes that Altimeter graphs."""
        lb_attrs = {}
        resp = client.describe_load_balancer_attributes(LoadBalancerName=lb_name)
        access_log_attrs = resp["LoadBalancerAttributes"]["AccessLog"]
        lb_attrs["AccessLogsEnabled"] = access_log_attrs["Enabled"]
        if "S3BucketName" in access_log_attrs:
            lb_attrs["AccessLogsS3Bucket"] = access_log_attrs["S3BucketName"]
        if "S3BucketPrefix" in access_log_attrs:
            lb_attrs["AccessLogsS3Prefix"] = access_log_attrs["S3BucketPrefix"]
        return lb_attrs
