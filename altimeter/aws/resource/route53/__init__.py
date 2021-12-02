"""Base class for Route53 resources."""
from altimeter.aws.resource.resource_spec import AWSResourceSpec, ScanGranularity


class Route53ResourceSpec(AWSResourceSpec):
    """Base class for Route53 resources."""

    scan_granularity = ScanGranularity.ACCOUNT
    service_name = "route53"
