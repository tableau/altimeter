"""Base class for IAM resources."""
from typing import Type

from altimeter.aws.resource.resource_spec import ScanGranularity, AWSResourceSpec


class IAMResourceSpec(AWSResourceSpec):
    """Base class for IAM resources."""

    service_name = "iam"
    scan_granularity = ScanGranularity.ACCOUNT

    @classmethod
    def generate_arn(
        cls: Type[AWSResourceSpec], resource_id: str, account_id: str = "", region: str = "",
    ) -> str:
        """Generate an ARN for this resource

        Args:
            account_id: resource account id
            region: resource region
            resource_id: resource id

        Returns:
            string containing resource arn.
        """
        return (
            ":".join(("arn", cls.provider_name, cls.service_name, "", account_id, cls.type_name))
            + f"/{resource_id}"
        )
