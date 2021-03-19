from unittest import TestCase
from unittest.mock import patch

import boto3
from botocore.exceptions import ClientError
from moto import mock_iam

from altimeter.aws.resource.iam.iam_oidc_provider import IAMOIDCProviderResourceSpec
from altimeter.aws.scan.aws_accessor import AWSAccessor
from altimeter.core.graph.links import LinkCollection, ResourceLink, SimpleLink
from altimeter.core.resource.resource import Resource


class TestIAMOIDCProvider(TestCase):
    @mock_iam
    def test_scan(self):
        account_id = "123456789012"
        region_name = "us-east-1"
        session = boto3.Session()
        client = session.client("iam")

        oidc_url = "https://oidc.eks.us-east-1.amazonaws.com/id/EXAMPLED539D4633E53DE1B716D3041E"
        oidc_client_ids = ["sts.amazonaws.com"]
        oidc_thumbprints = ["9999999999999999999999999999999999999999"]

        _ = client.create_open_id_connect_provider(
            Url=oidc_url, ClientIDList=oidc_client_ids, ThumbprintList=oidc_thumbprints,
        )

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        resources = IAMOIDCProviderResourceSpec.scan(scan_accessor=scan_accessor)

        expected_resources = [
            Resource(
                resource_id="arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-east-1.amazonaws.com/id/EXAMPLED539D4633E53DE1B716D3041E",
                type="aws:iam:oidc-provider",
                link_collection=LinkCollection(
                    simple_links=(
                        SimpleLink(
                            pred="url",
                            obj="oidc.eks.us-east-1.amazonaws.com/id/EXAMPLED539D4633E53DE1B716D3041E",
                        ),
                        SimpleLink(
                            pred="create_date",
                            obj=resources[0].link_collection.simple_links[1].obj,
                        ),
                        SimpleLink(pred="client_id", obj="sts.amazonaws.com"),
                        SimpleLink(
                            pred="thumbprint", obj="9999999999999999999999999999999999999999"
                        ),
                    ),
                    multi_links=None,
                    tag_links=None,
                    resource_links=(
                        ResourceLink(pred="account", obj="arn:aws::::account/123456789012"),
                    ),
                    transient_resource_links=None,
                ),
            )
        ]

        self.assertEqual(resources, expected_resources)

    @mock_iam
    def test_disappearing_oidc_provider_race_condition(self):
        account_id = "123456789012"
        region_name = "us-east-1"
        oidc_url = "https://oidc.eks.us-east-1.amazonaws.com/id/EXAMPLED539D4633E53DE1B716D3041E"
        oidc_client_ids = ["sts.amazonaws.com"]
        oidc_thumbprints = ["9999999999999999999999999999999999999999"]

        session = boto3.Session()

        client = session.client("iam")

        oidc_provider_resp = client.create_open_id_connect_provider(
            Url=oidc_url, ClientIDList=oidc_client_ids, ThumbprintList=oidc_thumbprints,
        )
        oidc_provider_arn = oidc_provider_resp["OpenIDConnectProviderArn"]

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        with patch(
            "altimeter.aws.resource.iam.iam_oidc_provider.IAMOIDCProviderResourceSpec"
            ".get_oidc_provider_details"
        ) as mock_get_oidc_provider_details:
            mock_get_oidc_provider_details.side_effect = ClientError(
                operation_name="GetOIDCProvider",
                error_response={
                    "Error": {
                        "Code": "NoSuchEntity",
                        "Message": f"OpenIDConnect Provider not found for arn {oidc_provider_arn}",
                    }
                },
            )
            resources = IAMOIDCProviderResourceSpec.scan(scan_accessor=scan_accessor)
            self.assertEqual(resources, [])
