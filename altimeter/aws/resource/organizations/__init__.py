"""Base class for AWS organizations resources."""
from typing import Type

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
