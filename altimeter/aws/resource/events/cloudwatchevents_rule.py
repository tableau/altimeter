"""Resource for CloudWatchEvents Rules"""
from typing import Any, Dict, List, Type

from botocore.client import BaseClient
from botocore.exceptions import ClientError

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
                try:
                    rule["Targets"] = list_targets_by_rule(client=client, rule_name=rule["Name"])
                    rules[resource_arn] = rule
                except ClientError as c_e:
                    error_code = getattr(c_e, "response", {}).get("Error", {}).get("Code", {})
                    if error_code != "ResourceNotFoundException":
                        raise c_e
        return ListFromAWSResult(resources=rules)


def list_targets_by_rule(client: BaseClient, rule_name: str) -> List[Dict[str, Any]]:
    """Return a list of target dicts for a given rule name"""
    targets = []
    targets_paginator = client.get_paginator("list_targets_by_rule")
    for targets_resp in targets_paginator.paginate(Rule=rule_name):
        targets += targets_resp.get("Targets", [])
    return targets
