"""Resource for IAM SAML Providers"""
import hashlib
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.iam import IAMResourceSpec
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class IAMSAMLProviderResourceSpec(IAMResourceSpec):
    """Resource for IAM SAML Providers"""

    type_name = "saml-provider"
    schema = Schema(
        ScalarField("Name"),
        ScalarField("ValidUntil"),
        ScalarField("CreateDate"),
        ScalarField("MetadataDocumentChecksum"),
    )

    @classmethod
    def list_from_aws(
        cls: Type["IAMSAMLProviderResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'saml_provider_1_arn': {saml_provider_1_dict},
             'saml_provider_2_arn': {saml_provider_2_dict},
             ...}

        Where the dicts represent results from list_saml_providers and additional info per
        saml_provider list_saml_providers. An additional 'Name' key is added."""
        saml_providers = {}
        resp = client.list_saml_providers()
        for saml_provider in resp.get("SAMLProviderList", []):
            resource_arn = saml_provider["Arn"]
            saml_provider["Name"] = "/".join(resource_arn.split("/")[1:])
            saml_provider_resp = client.get_saml_provider(SAMLProviderArn=resource_arn)
            saml_metadata_document = saml_provider_resp["SAMLMetadataDocument"]
            hash_object = hashlib.sha256(saml_metadata_document.encode())
            saml_provider["MetadataDocumentChecksum"] = hash_object.hexdigest()
            saml_providers[resource_arn] = saml_provider
        return ListFromAWSResult(resources=saml_providers)
