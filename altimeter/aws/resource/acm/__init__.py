"""Base class for acm resources."""

from altimeter.aws.resource.resource_spec import AWSResourceSpec


class ACMResourceSpec(AWSResourceSpec):
    """Base class for acm resources."""

    service_name = "acm"
