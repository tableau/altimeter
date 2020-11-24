"""Resource for VPC Endpoint Services"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.ec2 import EC2ResourceSpec
from altimeter.core.graph.field.scalar_field import ScalarField, EmbeddedScalarField
from altimeter.core.graph.field.list_field import ListField, AnonymousListField
from altimeter.core.graph.field.tags_field import TagsField
from altimeter.core.graph.schema import Schema


class VpcEndpointServiceResourceSpec(EC2ResourceSpec):
    """Resource for VPC Endpoint Services"""

    type_name = "vpc-endpoint-service"
    schema = Schema(
        AnonymousListField("ServiceType", ScalarField("ServiceType")),
        ScalarField("ServiceName"),
        ScalarField("ServiceState"),
        ScalarField("AcceptanceRequired"),
        ListField("AvailabilityZones", EmbeddedScalarField()),
        TagsField(),
    )

    @classmethod
    def list_from_aws(
        cls: Type["VpcEndpointServiceResourceSpec"],
        client: BaseClient,
        account_id: str,
        region: str,
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'vpc_endpoint_svc_1_arn': {vpc_endpoint_svc_1_dict},
             'vpc_endpoint_svc_2_arn': {vpc_endpoint_svc_2_dict},
             ...}

        Where the dicts represent results from describe_vpc_endpoint_service_configurations."""
        services = {}
        paginator = client.get_paginator("describe_vpc_endpoint_service_configurations")
        for resp in paginator.paginate():
            for service in resp.get("ServiceConfigurations", []):
                resource_arn = cls.generate_arn(
                    account_id=account_id, region=region, resource_id=service["ServiceId"]
                )
                services[resource_arn] = service
        return ListFromAWSResult(resources=services)
