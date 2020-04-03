"""AWSResourceSpec subclass for eks resources."""

from altimeter.aws.resource.resource_spec import AWSResourceSpec


class EKSResourceSpec(AWSResourceSpec):
    """AWSResourceSpec subclass for eks resources."""

    service_name = "eks"
