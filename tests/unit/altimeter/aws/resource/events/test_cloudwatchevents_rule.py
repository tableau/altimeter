import boto3
from botocore.exceptions import ClientError
from unittest import TestCase
from moto import mock_events
from unittest.mock import patch
from altimeter.aws.resource.events.cloudwatchevents_rule import EventsRuleResourceSpec
from altimeter.aws.scan.aws_accessor import AWSAccessor


class TestEventsRule(TestCase):
    @mock_events
    def test_disappearing_rule_race_condition(self):
        account_id = "123456789012"
        region_name = "us-east-1"
        rule_name = "test_rule"

        session = boto3.Session()
        client = session.client("events", region_name=region_name)
        client.put_rule(
            Name=rule_name,
            Description="Capture all events and forward them to 012345678901",
            EventPattern=f"""{{"account":["012345678901"]}}""",
            State="ENABLED",
        )

        scan_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region_name)
        with patch(
            "altimeter.aws.resource.events.cloudwatchevents_rule.list_targets_by_rule"
        ) as mock_list_targets_by_rule:
            mock_list_targets_by_rule.side_effect = ClientError(
                operation_name="ListTargetsByRule",
                error_response={
                    "Error": {
                        "Code": "ResourceNotFoundException",
                        "Message": f"Rule {rule_name} does not exist on EventBus default.",
                    }
                },
            )
            resources = EventsRuleResourceSpec.scan(scan_accessor=scan_accessor)
            self.assertEqual(resources, [])
