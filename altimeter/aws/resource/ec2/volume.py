"""Resource for EBSVolumes"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.ec2 import EC2ResourceSpec
from altimeter.aws.resource.ec2.instance import EC2InstanceResourceSpec
from altimeter.aws.resource.kms.key import KMSKeyResourceSpec
from altimeter.core.graph.field.dict_field import EmbeddedDictField
from altimeter.core.graph.field.list_field import ListField
from altimeter.core.graph.field.resource_link_field import (
    ResourceLinkField,
    TransientResourceLinkField,
)
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.field.tags_field import TagsField
from altimeter.core.graph.schema import Schema


class EBSVolumeResourceSpec(EC2ResourceSpec):
    """Resource for EBSVolumes"""

    type_name = "volume"
    schema = Schema(
        ScalarField("AvailabilityZone"),
        ScalarField("CreateTime"),
        ScalarField("Size"),
        ScalarField("State"),
        ScalarField("VolumeType"),
        ScalarField("Encrypted"),
        ListField(
            "Attachments",
            EmbeddedDictField(
                ScalarField("AttachTime"),
                ScalarField("State"),
                ScalarField("DeleteOnTermination"),
                ResourceLinkField("InstanceId", EC2InstanceResourceSpec),
            ),
            optional=True,
            alti_key="attachment",
        ),
        TransientResourceLinkField("KmsKeyId", KMSKeyResourceSpec, optional=True, value_is_id=True),
        TagsField(),
    )

    @classmethod
    def list_from_aws(
        cls: Type["EBSVolumeResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'volume_1_arn': {volume_1_dict},
             'volume_2_arn': {volume_2_dict},
             ...}

        Where the dicts represent results from describe_volumes."""
        volumes = {}
        paginator = client.get_paginator("describe_volumes")
        for resp in paginator.paginate():
            for volume in resp.get("Volumes", []):
                resource_arn = cls.generate_arn(
                    account_id=account_id, region=region, resource_id=volume["VolumeId"]
                )
                volumes[resource_arn] = volume
        return ListFromAWSResult(resources=volumes)
