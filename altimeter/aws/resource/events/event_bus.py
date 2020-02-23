"""Resource for CloudWatchEvents EventBusses"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.events import EventsResourceSpec
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class EventBusResourceSpec(EventsResourceSpec):
    """Resource for CloudWatchEvents EventBus"""

    type_name = "rule"
    schema = Schema(ScalarField("Name"), ScalarField("Arn"), ScalarField("Policy", optional=True),)

    @classmethod
    def list_from_aws(
        cls: Type["EventBusResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'event_bus_1_arn': {event_bus_1_dict},
             'event_bus_2_arn': {event_bus_2_dict},
             ...}

        Where the dicts represent results from describe_event_bus."""
        resp = client.describe_event_bus()
        arn = resp["Arn"]
        event_busses = {arn: resp}
        return ListFromAWSResult(resources=event_busses)
