"""Resource representing an AWS Organization."""
from typing import Type

from botocore.client import BaseClient

from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema
from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.organizations import OrganizationsResourceSpec


class OrgResourceSpec(OrganizationsResourceSpec):
    """Resource representing an AWS Org."""

    type_name = "organization"
    schema = Schema(ScalarField("MasterAccountId"), ScalarField("MasterAccountEmail"))

    @classmethod
    def get_full_type_name(cls: Type["OrgResourceSpec"]) -> str:
        return f"{cls.provider_name}:{cls.type_name}"

    @classmethod
    def list_from_aws(
        cls: Type["OrgResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'org_1_arn': {org_1_dict},
             'org_2_arn': {org_2_dict},
             ...}

        Where the dicts represent results from describe_organization."""
        resp = client.describe_organization()
        org = resp["Organization"]
        orgs = {org["Arn"]: org}
        return ListFromAWSResult(resources=orgs)
