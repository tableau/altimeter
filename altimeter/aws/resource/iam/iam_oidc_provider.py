"""Resource for IAM OIDC Providers"""
from typing import Type

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.iam import IAMResourceSpec
from altimeter.core.graph.field.scalar_field import ScalarField, EmbeddedScalarField
from altimeter.core.graph.field.list_field import ListField
from altimeter.core.graph.schema import Schema


class IAMOIDCProviderResourceSpec(IAMResourceSpec):
    """Resource for IAM OIDC Providers"""

    type_name = "oidc-provider"
    schema = Schema(
        ScalarField("Url"),
        ScalarField("CreateDate"),
        ListField("ClientIDList", EmbeddedScalarField(), alti_key="client_id"),
        ListField("ThumbprintList", EmbeddedScalarField(), alti_key="thumbprint"),
    )

    @classmethod
    def list_from_aws(
        cls: Type["IAMOIDCProviderResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'oidc_provider_1_arn': {oidc_provider_1_dict},
             'oidc_provider_2_arn': {oidc_provider_2_dict},
             ...}

        Where the dicts represent results from list_oidc_providers and additional info per
        oidc_provider list_oidc_providers. An additional 'Name' key is added."""
        oidc_providers = {}
        resp = client.list_open_id_connect_providers()
        for oidc_provider in resp.get("OpenIDConnectProviderList", []):
            resource_arn = oidc_provider["Arn"]
            try:
                oidc_provider_details = cls.get_oidc_provider_details(
                    client=client, arn=resource_arn
                )
                oidc_provider.update(oidc_provider_details)
                oidc_providers[resource_arn] = oidc_provider
            except ClientError as c_e:
                error_code = getattr(c_e, "response", {}).get("Error", {}).get("Code", {})
                if error_code != "NoSuchEntity":
                    raise c_e
        return ListFromAWSResult(resources=oidc_providers)

    @classmethod
    def get_oidc_provider_details(
        cls: Type["IAMOIDCProviderResourceSpec"], client: BaseClient, arn: str
    ) -> str:
        oidc_provider_resp = client.get_open_id_connect_provider(OpenIDConnectProviderArn=arn)
        return oidc_provider_resp
