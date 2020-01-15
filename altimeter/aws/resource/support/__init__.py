"""Base class for Support resources."""
from altimeter.aws.resource.resource_spec import ScanGranularity, AWSResourceSpec


class SupportResourceSpec(AWSResourceSpec):
    """Base class for Support resources."""

    service_name = "support"
    scan_granularity = ScanGranularity.ACCOUNT
    region_whitelist = ("us-east-1",)
