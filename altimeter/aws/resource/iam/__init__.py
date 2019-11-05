"""Base class for IAM resources."""

from altimeter.aws.resource.resource_spec import ScanGranularity, AWSResourceSpec


class IAMResourceSpec(AWSResourceSpec):
    """Base class for IAM resources."""

    service_name = "iam"
    scan_granularity = ScanGranularity.ACCOUNT
