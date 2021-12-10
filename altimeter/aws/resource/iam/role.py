"""Resource for IAM Roles"""
import copy
from typing import Any, Dict, List, Type

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.iam import IAMResourceSpec
from altimeter.aws.resource.iam.policy import IAMPolicyResourceSpec
from altimeter.aws.resource.util import policy_doc_dict_to_sorted_str
from altimeter.core.graph.field.dict_field import (
    EmbeddedDictField,
    AnonymousEmbeddedDictField,
    DictField,
)
from altimeter.core.graph.field.list_field import AnonymousListField, ListField
from altimeter.core.graph.field.resource_link_field import ResourceLinkField
from altimeter.core.graph.field.scalar_field import EmbeddedScalarField, ScalarField
from altimeter.core.graph.schema import Schema


class IAMRoleResourceSpec(IAMResourceSpec):
    """Resource for IAM Roles"""

    type_name = "role"
    parallel_scan = True
    schema = Schema(
        ScalarField("RoleName", "name"),
        ScalarField("MaxSessionDuration"),
        ScalarField("Description", optional=True),
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
        DictField(
            "AssumeRolePolicyDocument",
            ScalarField("Version"),
            ListField(
                "Statement",
                EmbeddedDictField(
                    ScalarField("Effect"),
                    ListField("Action", EmbeddedScalarField(), allow_scalar=True),
                    DictField(
                        "Principal",
                        ListField("AWS", EmbeddedScalarField(), optional=True, allow_scalar=True,),
                        ListField(
                            "Federated", EmbeddedScalarField(), optional=True, allow_scalar=True,
                        ),
                        ListField(
                            "Service", EmbeddedScalarField(), optional=True, allow_scalar=True
                        ),
                    ),
                ),
            ),
        ),
        ScalarField("AssumeRolePolicyDocumentText"),
    )

    @classmethod
    def list_from_aws(
        cls: Type["IAMRoleResourceSpec"], client: BaseClient, account_id: str, region: str,
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'role_1_arn': {role_1_dict},
             'role_2_arn': {role_2_dict},
             ...}

        Where the dicts represent results from list_roles and additional info per role from
        list_targets_by_role."""
        roles = {}
        paginator = client.get_paginator("list_roles")
        for resp in paginator.paginate():
            for role in resp.get("Roles", []):
                role_name = role["RoleName"]
                assume_role_policy_document = copy.deepcopy(role["AssumeRolePolicyDocument"])
                assume_role_policy_document_text = policy_doc_dict_to_sorted_str(
                    assume_role_policy_document
                )
                role["AssumeRolePolicyDocumentText"] = assume_role_policy_document_text
                for statement in assume_role_policy_document.get("Statement", []):
                    for obj in statement.get("Condition", {}).values():
                        for obj_key in obj.keys():
                            if obj_key.lower() == "sts:externalid":
                                obj[obj_key] = "REMOVED"
                try:
                    attached_policies = get_attached_role_policies(client, role_name)
                    role["PolicyAttachments"] = attached_policies
                    resource_arn = role["Arn"]
                    roles[resource_arn] = role
                    embedded_policies = get_embedded_role_policies(client, role_name)
                    role["EmbeddedPolicy"] = embedded_policies
                except ClientError as c_e:
                    error_code = getattr(c_e, "response", {}).get("Error", {}).get("Code", {})
                    if error_code != "NoSuchEntity":
                        raise c_e
        return ListFromAWSResult(resources=roles)


def get_attached_role_policies(client: BaseClient, role_name: str) -> List[Dict[str, Any]]:
    """Get attached role policies"""
    policies = []
    paginator = client.get_paginator("list_attached_role_policies")
    for resp in paginator.paginate(RoleName=role_name):
        for policy in resp.get("AttachedPolicies", []):
            policies.append(policy)
    return policies


def get_embedded_role_policies(client: BaseClient, role_name: str) -> List[Dict[str, Any]]:
    """Get attached embedded policies"""
    policies = []
    paginator = client.get_paginator("list_role_policies")
    for resp in paginator.paginate(RoleName=role_name):
        for policy_name in resp.get("PolicyNames", []):
            policy = get_embedded_role_policy(client, role_name, policy_name)
            policies.append(policy)
    return policies


def get_embedded_role_policy(
    client: BaseClient, role_name: str, policy_name: str
) -> Dict[str, str]:
    """Get attached embedded policies"""
    resp = client.get_role_policy(RoleName=role_name, PolicyName=policy_name)
    policy_document = resp.get("PolicyDocument")
    policy_name = resp.get("PolicyName")
    policy_document = policy_doc_dict_to_sorted_str(policy_document)
    return {
        "PolicyName": policy_name,
        "PolicyDocument": policy_document,
    }
