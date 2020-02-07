"""ResourceSpec classes for classic elb resources."""
from altimeter.aws.resource.resource_spec import AWSResourceSpec


class ELBV1ResourceSpec(AWSResourceSpec):
    """Abstract base for ResourceSpec classes for classic elb resources."""

    service_name = "elb"

    @classmethod
    def get_client_name(cls) -> str:
        return "elb"
