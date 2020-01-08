"""Resource for Account Password Policy"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.iam import IAMResourceSpec
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class IAMAccountPasswordPolicyResourceSpec(IAMResourceSpec):
    """Resource for Account Password Policy"""

    DEFAULT_PASSWORD_POLICY_NAME = "default"

    type_name = "account-password-policy"
    schema = Schema(
        ScalarField("MinimumPasswordLength"),
        ScalarField("RequireSymbols"),
        ScalarField("RequireNumbers"),
        ScalarField("RequireUppercaseCharacters"),
        ScalarField("RequireLowercaseCharacters"),
        ScalarField("AllowUsersToChangePassword"),
        ScalarField("ExpirePasswords"),
        ScalarField("MaxPasswordAge", optional=True),
        ScalarField("PasswordReusePrevention", optional=True),
        ScalarField("HardExpiry", optional=True),
    )

    @classmethod
    def list_from_aws(
        cls: Type["IAMAccountPasswordPolicyResourceSpec"],
        client: BaseClient,
        account_id: str,
        region: str,
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'account_password_policy_1_arn': {account_password_policy_1_dict}}

        Where the dicts represent results from get_account_password_policy."""
        password_policies = {}
        try:
            resp = client.get_account_password_policy()
        except client.exceptions.NoSuchEntityException:
            resp = {}  # Indicates no policy is set for the account

        policy = resp.get("PasswordPolicy", {})

        if policy:
            resource_arn = cls.generate_arn(
                account_id=account_id, resource_id=cls.DEFAULT_PASSWORD_POLICY_NAME
            )
            password_policies[resource_arn] = policy
        return ListFromAWSResult(resources=password_policies)
