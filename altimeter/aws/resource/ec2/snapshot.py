"""Resource for EBSSnapshots"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.ec2 import EC2ResourceSpec
from altimeter.aws.resource.ec2.volume import EBSVolumeResourceSpec
from altimeter.aws.resource.kms.key import KMSKeyResourceSpec
from altimeter.core.graph.field.resource_link_field import TransientResourceLinkField
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.field.tags_field import TagsField
from altimeter.core.graph.schema import Schema


class EBSSnapshotResourceSpec(EC2ResourceSpec):
    """Resource for EBSSnapshots"""

    type_name = "snapshot"
    schema = Schema(
        ScalarField("VolumeSize"),
        ScalarField("Encrypted"),
        TransientResourceLinkField("KmsKeyId", KMSKeyResourceSpec, optional=True, value_is_id=True),
        TransientResourceLinkField("VolumeId", EBSVolumeResourceSpec),
        TagsField(),
    )

    @classmethod
    def list_from_aws(
        cls: Type["EBSSnapshotResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'snapshot_1_arn': {snapshot_1_dict},
             'snapshot_2_arn': {snapshot_2_dict},
             ...}

        Where the dicts represent results from describe_snapshots."""
        snapshots = {}
        paginator = client.get_paginator("describe_snapshots")
        for resp in paginator.paginate(OwnerIds=["self"]):
            for snapshot in resp.get("Snapshots", []):
                resource_arn = cls.generate_arn(
                    account_id=account_id, region=region, resource_id=snapshot["SnapshotId"]
                )
                snapshots[resource_arn] = snapshot
        return ListFromAWSResult(resources=snapshots)
