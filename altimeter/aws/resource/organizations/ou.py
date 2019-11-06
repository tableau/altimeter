"""Resource representing an AWS Organizational Unit."""
from typing import Any, Dict, List, Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.organizations import OrganizationsResourceSpec
from altimeter.aws.resource.organizations.org import OrgResourceSpec
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.field.resource_link_field import ResourceLinkField
from altimeter.core.graph.schema import Schema


class OUResourceSpec(OrganizationsResourceSpec):
    """Resource representing an AWS OU."""

    type_name = "ou"
    schema = Schema(
        ScalarField("Path"), ResourceLinkField("OrganizationArn", OrgResourceSpec, value_is_id=True)
    )

    @classmethod
    def get_full_type_name(cls: Type["OUResourceSpec"]) -> str:
        return f"{cls.provider_name}:{cls.type_name}"

    @classmethod
    def list_from_aws(
        cls: Type["OUResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'ou_1_arn': {ou_1_dict},
             'ou_2_arn': {ou_2_dict},
             ...}

        Where the dicts represent results from list_organizational_units_for_parent
        with some additional info 'Path') tagged on."""
        org_resp = client.describe_organization()
        org_arn = org_resp["Organization"]["Arn"]
        ous = {}
        paginator = client.get_paginator("list_roots")
        for resp in paginator.paginate():
            for root in resp["Roots"]:
                root_id, root_arn = root["Id"], root["Arn"]
                root_path = f"/{root['Name']}"
                ous[root_arn] = root
                ous[root_arn]["OrganizationArn"] = org_arn
                ous[root_arn]["Path"] = root_path
                ou_details = cls._recursively_get_ou_details_for_parent(
                    client=client, parent_id=root_id, parent_path=root_path
                )
                for ou_detail in ou_details:
                    arn = ou_detail["Arn"]
                    ou_detail["OrganizationArn"] = org_arn
                    ous[arn] = ou_detail
        return ListFromAWSResult(resources=ous)

    @classmethod
    def _recursively_get_ou_details_for_parent(
        cls: Type["OUResourceSpec"], client: BaseClient, parent_id: str, parent_path: str
    ) -> List[Dict[str, Any]]:
        ous = []
        paginator = client.get_paginator("list_organizational_units_for_parent")
        for resp in paginator.paginate(ParentId=parent_id):
            for ou in resp["OrganizationalUnits"]:
                ou_id = ou["Id"]
                path = f"{parent_path}/{ou['Name']}"
                ou["Path"] = path
                ous.append(ou)
                ous += cls._recursively_get_ou_details_for_parent(
                    client=client, parent_id=ou_id, parent_path=path
                )
        return ous
