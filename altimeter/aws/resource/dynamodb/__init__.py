"""Base class for DynamoDB resources."""

from altimeter.aws.resource.resource_spec import AWSResourceSpec


class DynamoDBResourceSpec(AWSResourceSpec):
    """Base class for DynamoDB resources."""

    service_name = "dynamodb"
