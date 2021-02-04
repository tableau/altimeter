import boto3
from botocore.exceptions import ClientError
from unittest import TestCase
from moto import mock_iam
from unittest.mock import patch
from altimeter.aws.resource.iam.iam_saml_provider import IAMSAMLProviderResourceSpec
from altimeter.aws.scan.aws_accessor import AWSAccessor


class TestIAMSAMLProvider(TestCase):
    @mock_iam
    def test_disappearing_saml_provider_race_condition(self):
        account_id = "123456789012"
        saml_provider_name = "foo"
        region_name = "us-east-1"

        session = boto3.Session()

        client = session.client("iam")

        saml_provider_resp = client.create_saml_provider(
            Name=saml_provider_name, SAMLMetadataDocument="a" * 1024
        )
        saml_provider_arn = saml_provider_resp["SAMLProviderArn"]

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        with patch(
            "altimeter.aws.resource.iam.iam_saml_provider.IAMSAMLProviderResourceSpec.get_saml_provider_metadata_doc"
        ) as mock_get_saml_provider_metadata_doc:
            mock_get_saml_provider_metadata_doc.side_effect = ClientError(
                operation_name="GetSAMLProvider",
                error_response={
                    "Error": {
                        "Code": "NoSuchEntity",
                        "Message": f"GetSAMLProvider operation: Manifest not found for arn {saml_provider_arn}",
                    }
                },
            )
            resources = IAMSAMLProviderResourceSpec.scan(scan_accessor=scan_accessor)
            self.assertEqual(resources, [])
