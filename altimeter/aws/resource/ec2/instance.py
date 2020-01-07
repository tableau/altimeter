"""Resource for EC2Instances"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.ec2 import EC2ResourceSpec
from altimeter.aws.resource.ec2.image import EC2ImageResourceSpec
from altimeter.aws.resource.ec2.security_group import SecurityGroupResourceSpec
from altimeter.aws.resource.ec2.subnet import SubnetResourceSpec
from altimeter.aws.resource.ec2.vpc import VPCResourceSpec
from altimeter.aws.resource.iam.instance_profile import InstanceProfileResourceSpec
from altimeter.core.graph.field.dict_field import AnonymousDictField, AnonymousEmbeddedDictField
from altimeter.core.graph.field.list_field import AnonymousListField
from altimeter.core.graph.field.resource_link_field import (
    ResourceLinkField,
    TransientResourceLinkField,
)
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.field.tags_field import TagsField
from altimeter.core.graph.schema import Schema


class EC2InstanceResourceSpec(EC2ResourceSpec):
    """Resource for EC2Instances"""

    type_name = "instance"
    schema = Schema(
        TransientResourceLinkField("ImageId", EC2ImageResourceSpec),
        ScalarField("InstanceType"),
        ScalarField("LaunchTime"),
        AnonymousDictField("State", ScalarField("Name", "state")),
        ScalarField("Platform", optional=True),
        ScalarField("PrivateIpAddress", optional=True),
        ScalarField("PublicIpAddress", optional=True),
        ResourceLinkField("VpcId", VPCResourceSpec, optional=True),
        ResourceLinkField("SubnetId", SubnetResourceSpec, optional=True),
        AnonymousListField(
            "SecurityGroups",
            AnonymousEmbeddedDictField(ResourceLinkField("GroupId", SecurityGroupResourceSpec)),
        ),
        AnonymousDictField(
            "IamInstanceProfile",
            TransientResourceLinkField(
                "Arn", InstanceProfileResourceSpec, alti_key="instance_profile", value_is_id=True
            ),
            optional=True,
        ),
        TagsField(),
    )

    @classmethod
    def list_from_aws(
        cls: Type["EC2InstanceResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'instance_1_arn': {instance_1_dict},
             'instance_2_arn': {instance_2_dict},
             ...}

        Where the dicts represent results from describe_instances."""
        paginator = client.get_paginator("describe_instances")
        instances = {}
        for resp in paginator.paginate():
            for reservation in resp.get("Reservations", []):
                for instance in reservation.get("Instances", []):
                    resource_arn = cls.generate_arn(
                        account_id=account_id, region=region, resource_id=instance["InstanceId"]
                    )
                    instances[resource_arn] = instance
        return ListFromAWSResult(resources=instances)
