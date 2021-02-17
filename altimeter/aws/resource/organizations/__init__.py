"""Base class for AWS organizations resources."""
from typing import Type, List, Dict, Any

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ScanGranularity, AWSResourceSpec


class OrganizationsResourceSpec(AWSResourceSpec):
    """Base class for AWS organizations resources."""

    service_name = "organizations"
    scan_granularity = ScanGranularity.ACCOUNT

    @classmethod
    def skip_resource_scan(
        cls: Type["OrganizationsResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> bool:
        """Return a bool indicating whether this resource class scan should be skipped,
        in this case skip if the current account is not an org master."""
        resp = client.describe_organization()
        return resp["Organization"]["MasterAccountId"] != account_id


def recursively_get_ou_details_for_parent(
    client: BaseClient, parent_id: str, parent_path: str
) -> List[Dict[str, Any]]:
    ous = []
    paginator = client.get_paginator("list_organizational_units_for_parent")
    for resp in paginator.paginate(ParentId=parent_id):
        for ou in resp["OrganizationalUnits"]:
            ou_id = ou["Id"]
            path = f"{parent_path}/{ou['Name']}"
            ou["Path"] = path
            ous.append(ou)
            ous += recursively_get_ou_details_for_parent(
                client=client, parent_id=ou_id, parent_path=path
            )
    return ous
