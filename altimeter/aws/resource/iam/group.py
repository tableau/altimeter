"""Resource for IAM Groups"""
from typing import Any, Dict, List, Type

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.iam import IAMResourceSpec
from altimeter.aws.resource.iam.policy import IAMPolicyResourceSpec
from altimeter.aws.resource.util import policy_doc_dict_to_sorted_str
from altimeter.aws.resource.iam.user import IAMUserResourceSpec
from altimeter.core.graph.field.list_field import AnonymousListField, ListField
from altimeter.core.graph.field.resource_link_field import ResourceLinkField
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.field.dict_field import (
    EmbeddedDictField,
    AnonymousEmbeddedDictField,
)
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
        AnonymousListField(
            "PolicyAttachments",
            AnonymousEmbeddedDictField(
                ResourceLinkField(
                    "PolicyArn",
                    IAMPolicyResourceSpec,
                    optional=True,
                    value_is_id=True,
                    alti_key="attached_policy",
                )
            ),
        ),
        ListField(
            "EmbeddedPolicy",
            EmbeddedDictField(ScalarField("PolicyName"), ScalarField("PolicyDocument"),),
            optional=True,
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
                group_name = group["GroupName"]
                try:
                    group["Users"] = cls.get_group_users(client=client, group_name=group_name)
                    groups[resource_arn] = group
                    attached_policies = get_attached_group_policies(client, group_name)
                    group["PolicyAttachments"] = attached_policies
                    embedded_policies = get_embedded_group_policies(client, group_name)
                    group["EmbeddedPolicy"] = embedded_policies
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


def get_attached_group_policies(client: BaseClient, group_name: str) -> List[Dict[str, Any]]:
    """Get attached group policies"""
    policies = []
    paginator = client.get_paginator("list_attached_group_policies")
    for resp in paginator.paginate(GroupName=group_name):
        for policy in resp.get("AttachedPolicies", []):
            policies.append(policy)
    return policies


def get_embedded_group_policies(client: BaseClient, group_name: str) -> List[Dict[str, Any]]:
    """Get embedded group policies"""
    policies = []
    paginator = client.get_paginator("list_group_policies")
    for resp in paginator.paginate(GroupName=group_name):
        for policy_name in resp.get("PolicyNames", []):
            policy = get_embedded_group_policy(client, group_name, policy_name)
            policies.append(policy)
    return policies


def get_embedded_group_policy(
    client: BaseClient, group_name: str, policy_name: str
) -> Dict[str, str]:
    """Get embedded group policy"""
    resp = client.get_group_policy(GroupName=group_name, PolicyName=policy_name)
    policy_document = resp.get("PolicyDocument")
    policy_name = resp.get("PolicyName")
    policy_document = policy_doc_dict_to_sorted_str(policy_document)
    return {
        "PolicyName": policy_name,
        "PolicyDocument": policy_document,
    }
