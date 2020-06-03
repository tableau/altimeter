"""Resource for Instance Profiles"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.iam import IAMResourceSpec
from altimeter.aws.resource.iam.role import IAMRoleResourceSpec
from altimeter.core.graph.field.dict_field import AnonymousEmbeddedDictField
from altimeter.core.graph.field.list_field import AnonymousListField
from altimeter.core.graph.field.resource_link_field import ResourceLinkField
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class InstanceProfileResourceSpec(IAMResourceSpec):
    """Resource for Instance Profiles"""

    type_name = "instance-profile"
    schema = Schema(
        ScalarField("InstanceProfileName", alti_key="name"),
        AnonymousListField(
            "Roles",
            AnonymousEmbeddedDictField(
                ResourceLinkField(
                    "Arn", IAMRoleResourceSpec, value_is_id=True, alti_key="attached_role"
                )
            ),
        ),
    )

    @classmethod
    def list_from_aws(
        cls: Type["InstanceProfileResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'instance_profile_1_arn': {instance_profile_1_dict},
             'instance_profile_2_arn': {instance_profile_2_dict},
             ...}

        Where the dicts represent results from list_instance_profiles."""
        paginator = client.get_paginator("list_instance_profiles")
        instance_profiles = {}
        for resp in paginator.paginate():
            for instance_profile in resp.get("InstanceProfiles", []):
                resource_arn = instance_profile["Arn"]
                instance_profiles[resource_arn] = instance_profile
        return ListFromAWSResult(resources=instance_profiles)
