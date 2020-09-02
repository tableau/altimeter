"""Resource for IAM Groups"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.iam import IAMResourceSpec
from altimeter.aws.resource.iam.user import IAMUserResourceSpec
from altimeter.core.graph.field.dict_field import AnonymousEmbeddedDictField
from altimeter.core.graph.field.list_field import AnonymousListField
from altimeter.core.graph.field.resource_link_field import ResourceLinkField
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class IAMGroupResourceSpec(IAMResourceSpec):
    """Resource for IAM Groups"""

    type_name = "group"
    schema = Schema(
        ScalarField("GroupName", "name"),
        ScalarField("GroupId"),
        ScalarField("CreateDate"),
        AnonymousListField(
            "Users",
            AnonymousEmbeddedDictField(
                ResourceLinkField("Arn", IAMUserResourceSpec, value_is_id=True, alti_key="user")
            ),
        ),
    )

    @classmethod
    def list_from_aws(
        cls: Type["IAMGroupResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'group_1_arn': {group_1_dict},
             'group_2_arn': {group_2_dict},
             ...}

        Where the dicts represent results from list_groups."""
        groups = {}
        paginator = client.get_paginator("list_groups")
        for resp in paginator.paginate():
            for group in resp.get("Groups", []):
                resource_arn = group["Arn"]
                group_resp = client.get_group(GroupName=group["GroupName"])
                group["Users"] = group_resp["Users"]
                groups[resource_arn] = group
        return ListFromAWSResult(resources=groups)
