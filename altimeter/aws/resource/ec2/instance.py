"""Resource for EC2Instances"""
from typing import Dict, Set, Type

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
        ScalarField("Name", optional=True),
        TransientResourceLinkField("ImageId", EC2ImageResourceSpec),
        ScalarField("AMIId"),
        ScalarField("AMIName"),
        ScalarField("KeyName", optional=True),
        AnonymousDictField("Placement", ScalarField("AvailabilityZone"), ScalarField("Tenancy")),
        ScalarField("InstanceType"),
        ScalarField("LaunchTime"),
        AnonymousDictField("State", ScalarField("Name", "state")),
        ScalarField("Platform", optional=True),
        ScalarField("PrivateIpAddress", optional=True),
        ScalarField("PrivateDnsName", optional=True),
        ScalarField("PublicIpAddress", optional=True),
        ScalarField("PublicDnsName", optional=True),
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
        ami_ids: Set[str] = set()
        for resp in paginator.paginate():
            for reservation in resp.get("Reservations", []):
                for instance in reservation.get("Instances", []):
                    ami_id = instance["ImageId"]
                    instance["AMIId"] = ami_id
                    ami_ids.add(ami_id)
                    resource_arn = cls.generate_arn(
                        account_id=account_id, region=region, resource_id=instance["InstanceId"]
                    )
                    instances[resource_arn] = instance
                    for tag in instance.get("Tags", []):
                        if tag["Key"].lower() == "name":
                            instance["Name"] = tag["Value"]
                            break
        # now fill ami name and ami id
        if ami_ids:
            ami_ids_names = get_ami_ids_names(client=client, ami_ids=ami_ids)
            for instance_dict in instances.values():
                instance_dict["AMIName"] = ami_ids_names.get(
                    instance_dict["AMIId"],
                    "EC2 can't retrieve the name because the AMI was either deleted or made private.",
                )
        return ListFromAWSResult(resources=instances)


def get_ami_ids_names(client: BaseClient, ami_ids: Set[str]) -> Dict[str, str]:
    """Get a dict of ami ids to ami names"""
    resp = client.describe_images(ImageIds=list(ami_ids))
    images = resp["Images"]
    ami_ids_names = {image["ImageId"]: image["Name"] for image in images}
    return ami_ids_names
