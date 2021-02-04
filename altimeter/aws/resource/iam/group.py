"""Resource for IAM Groups"""
from typing import Any, Dict, List, Type

from botocore.client import BaseClient
from botocore.exceptions import ClientError

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
                try:
                    group["Users"] = cls.get_group_users(
                        client=client, group_name=group["GroupName"]
                    )
                    groups[resource_arn] = group
                except ClientError as c_e:
                    error_code = getattr(c_e, "response", {}).get("Error", {}).get("Code", {})
                    if error_code != "NoSuchEntity":
                        raise c_e
        return ListFromAWSResult(resources=groups)

    @classmethod
    def get_group_users(
        cls: Type["IAMGroupResourceSpec"], client: BaseClient, group_name: str
    ) -> List[Dict[str, Any]]:
        group_resp = client.get_group(GroupName=group_name)
        return group_resp["Users"]
