"""Resource for CloudWatchEvents Rules"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.events import EventsResourceSpec
from altimeter.core.graph.field.dict_field import EmbeddedDictField
from altimeter.core.graph.field.list_field import ListField
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class EventsRuleResourceSpec(EventsResourceSpec):
    """Resource for CloudWatchEvents Rules"""

    type_name = "rule"
    schema = Schema(
        ScalarField("Name"),
        ScalarField("State"),
        ScalarField("EventPattern", optional=True),
        ScalarField("ScheduleExpression", optional=True),
        ListField(
            "Targets",
            EmbeddedDictField(
                ScalarField("Id", "name"), ScalarField("Arn"), ScalarField("RoleArn", optional=True)
            ),
            alti_key="target",
        ),
    )

    @classmethod
    def list_from_aws(
        cls: Type["EventsRuleResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'rule_1_arn': {rule_1_dict},
             'rule_2_arn': {rule_2_dict},
             ...}

        Where the dicts represent results from list_rules and additional info per rule from
        list_targets_by_rule."""
        rules = {}
        paginator = client.get_paginator("list_rules")
        for resp in paginator.paginate():
            for rule in resp.get("Rules", []):
                resource_arn = rule["Arn"]
                rules[resource_arn] = rule
                targets_paginator = client.get_paginator("list_targets_by_rule")
                rule["Targets"] = []
                for targets_resp in targets_paginator.paginate(Rule=rule["Name"]):
                    rule["Targets"] += targets_resp.get("Targets", [])
        return ListFromAWSResult(resources=rules)
