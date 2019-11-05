"""Base class for S3 resources."""
from altimeter.aws.resource.resource_spec import AWSResourceSpec


class S3ResourceSpec(AWSResourceSpec):
    """Base class for S3 resources."""

    service_name = "s3"
