"""Base class for lambda resources."""

from altimeter.aws.resource.resource_spec import AWSResourceSpec


class LambdaResourceSpec(AWSResourceSpec):
    """Base class for lambda resources."""

    service_name = "lambda"
