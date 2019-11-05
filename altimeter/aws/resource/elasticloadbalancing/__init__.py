"""ResourceSpec classes for elb resources."""
from altimeter.aws.resource.resource_spec import AWSResourceSpec


class ElasticLoadBalancingResourceSpec(AWSResourceSpec):
    """Abstract base for ResourceSpec classes for elb resources."""

    service_name = "elasticloadbalancing"

    @classmethod
    def get_client_name(cls) -> str:
        return "elbv2"
