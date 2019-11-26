"""Resource for IAM Policies"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.iam import IAMResourceSpec
from altimeter.aws.resource.util import policy_doc_dict_to_sorted_str
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class IAMPolicyResourceSpec(IAMResourceSpec):
    """Resource for IAM Policies"""

    type_name = "policy"
    schema = Schema(
        ScalarField("PolicyName", "name"),
        ScalarField("PolicyId"),
        ScalarField("DefaultVersionPolicyDocumentText"),
    )

    @classmethod
    def list_from_aws(
        cls: Type["IAMPolicyResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'role_1_arn': {role_1_dict},
             'role_2_arn': {role_2_dict},
             ...}

        Where the dicts represent results from list_policies and additional info per role from
        list_targets_by_role."""
        policies = {}
        paginator = client.get_paginator("list_policies")

        # Scope parameter can be modified to return AWS managed policies
        # Currently scoped to user defined policies only
        for resp in paginator.paginate():
            for policy in resp.get("Policies", []):
                resource_arn = policy["Arn"]
                default_policy_version = policy["DefaultVersionId"]
                policy_version_resp = client.get_policy_version(
                    PolicyArn=resource_arn, VersionId=default_policy_version
                )
                default_policy_version_document_text = policy_version_resp["PolicyVersion"][
                    "Document"
                ]
                policy["DefaultVersionPolicyDocumentText"] = policy_doc_dict_to_sorted_str(
                    default_policy_version_document_text
                )
                policies[resource_arn] = policy
        return ListFromAWSResult(resources=policies)
