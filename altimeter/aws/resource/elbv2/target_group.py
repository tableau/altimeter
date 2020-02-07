"""Resource for target groups"""
from typing import Any, Dict, List, Type

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.ec2.vpc import VPCResourceSpec
from altimeter.aws.resource.elbv2 import ELBV2ResourceSpec
from altimeter.aws.resource.elbv2.load_balancer import LoadBalancerResourceSpec
from altimeter.core.exceptions import AltimeterException
from altimeter.core.graph.field.dict_field import AnonymousDictField, EmbeddedDictField
from altimeter.core.graph.field.list_field import ListField
from altimeter.core.graph.field.resource_link_field import (
    EmbeddedResourceLinkField,
    TransientResourceLinkField,
)
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class TargetGroupAccessDeniedException(AltimeterException):
    pass


class TargetGroupResourceSpec(ELBV2ResourceSpec):
    """Resource for target group"""

    type_name = "targetgroup"
    schema = Schema(
        ScalarField("TargetGroupName"),
        ScalarField("Protocol", optional=True),
        ScalarField("Port", optional=True),
        TransientResourceLinkField("VpcId", VPCResourceSpec, optional=True),
        ScalarField("HealthCheckProtocol", optional=True),
        ScalarField("HealthCheckPort", optional=True),
        ScalarField("HealthCheckEnabled"),
        ListField(
            "LoadBalancerArns",
            EmbeddedResourceLinkField(LoadBalancerResourceSpec, value_is_id=True),
        ),
        ScalarField("TargetType"),
        ListField(
            "TargetHealthDescriptions",
            EmbeddedDictField(
                AnonymousDictField(
                    "Target",
                    ScalarField("Id", alti_key="target_id"),
                    ScalarField("Port", alti_key="target_port", optional=True),
                    ScalarField("AvailabilityZone", alti_key="target_az", optional=True),
                ),
                ScalarField("HealthCheckPort", optional=True),
                AnonymousDictField(
                    "TargetHealth",
                    ScalarField("State"),
                    ScalarField("Reason", optional=True),
                    ScalarField("Description", optional=True),
                    optional=True,
                ),
            ),
        ),
    )

    @classmethod
    def list_from_aws(
        cls: Type["TargetGroupResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'target_group_1_arn': {target_group_1_dict},
             'target_group_2_arn': {target_group_2_dict},
             ...}

        Where the dicts represent results from describe_target_groups."""
        paginator = client.get_paginator("describe_target_groups")
        resources = {}
        for resp in paginator.paginate():
            for resource in resp.get("TargetGroups", []):
                resource_arn = resource["TargetGroupArn"]
                resource["TargetHealthDescriptions"] = get_target_group_health(client, resource_arn)
                resources[resource_arn] = resource
        return ListFromAWSResult(resources=resources)


def get_target_group_health(client: BaseClient, target_group_arn: str) -> List[Dict[str, Any]]:
    """Describes target health for a given target group"""
    try:
        return client.describe_target_health(TargetGroupArn=target_group_arn).get(
            "TargetHealthDescriptions", []
        )
    except ClientError as c_e:
        response_error = getattr(c_e, "response", {}).get("Error", {})
        error_code = response_error.get("Code", "")
        if error_code == "AccessDenied":
            raise TargetGroupAccessDeniedException(
                f"Error getting encryption configuration for {target_group_arn}: {response_error}",
                c_e,
            )
        raise c_e
