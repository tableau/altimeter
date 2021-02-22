"""Resource for IAM Policies"""
from typing import Any, Dict, Type

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.iam import IAMResourceSpec
from altimeter.aws.resource.util import policy_doc_dict_to_sorted_str
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class IAMPolicyResourceSpec(IAMResourceSpec):
    """Resource for user-managed IAM Policies"""

    type_name = "policy"
    parallel_scan = True
    schema = Schema(
        ScalarField("PolicyName", "name"),
        ScalarField("PolicyId"),
        ScalarField("DefaultVersionId"),
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

        for resp in paginator.paginate(Scope="Local"):
            for policy in resp.get("Policies", []):
                resource_arn = policy["Arn"]
                default_policy_version = policy["DefaultVersionId"]
                try:
                    default_policy_version_document_text = cls.get_policy_version_document_text(
                        client=client,
                        policy_arn=resource_arn,
                        policy_version=default_policy_version,
                    )
                    policy["DefaultVersionPolicyDocumentText"] = policy_doc_dict_to_sorted_str(
                        default_policy_version_document_text
                    )
                    policies[resource_arn] = policy
                except ClientError as c_e:
                    error_code = getattr(c_e, "response", {}).get("Error", {}).get("Code", {})
                    if error_code != "NoSuchEntity":
                        raise c_e

        return ListFromAWSResult(resources=policies)

    @classmethod
    def get_policy_version_document_text(
        cls: Type["IAMPolicyResourceSpec"],
        client: BaseClient,
        policy_arn: str,
        policy_version: str,
    ) -> Dict[str, Any]:
        policy_version_resp = client.get_policy_version(
            PolicyArn=policy_arn, VersionId=policy_version
        )
        return policy_version_resp["PolicyVersion"]["Document"]


class IAMAWSManagedPolicyResourceSpec(IAMResourceSpec):
    """Resource for AWS-managed IAM Policies"""

    type_name = "policy"
    schema = Schema(ScalarField("PolicyName", "name"), ScalarField("PolicyId"))

    @classmethod
    def list_from_aws(
        cls: Type["IAMAWSManagedPolicyResourceSpec"],
        client: BaseClient,
        account_id: str,
        region: str,
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'role_1_arn': {role_1_dict},
             'role_2_arn': {role_2_dict},
             ...}

        Where the dicts represent results from list_policies and additional info per role from
        list_targets_by_role."""
        policies = {}
        paginator = client.get_paginator("list_policies")

        for resp in paginator.paginate(Scope="AWS", OnlyAttached=True):
            for policy in resp.get("Policies", []):
                resource_arn = policy["Arn"]
                policies[resource_arn] = policy
        return ListFromAWSResult(resources=policies)
