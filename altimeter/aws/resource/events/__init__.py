"""Base class for CloudWatch Events resources."""

from altimeter.aws.resource.resource_spec import AWSResourceSpec


class EventsResourceSpec(AWSResourceSpec):
    """Base class for CloudWatch Events resources."""

    service_name = "events"
