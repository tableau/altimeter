"""Resource for CommandInvocations"""
from datetime import datetime, timedelta, timezone
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.ssm import SSMResourceSpec
from altimeter.aws.resource.ec2.instance import EC2InstanceResourceSpec
from altimeter.aws.settings import SSM_COMMAND_INVOCATION_LOOKBACK_MIN
from altimeter.core.graph.field.resource_link_field import TransientResourceLinkField
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class CommandInvocationResourceSpec(SSMResourceSpec):
    """Resource for SSM Command Invocations"""

    type_name = "command-invocation"
    schema = Schema(
        TransientResourceLinkField("InstanceId", EC2InstanceResourceSpec),
        ScalarField("InstanceName"),
        ScalarField("DocumentName"),
        ScalarField("RequestedDateTime"),
        ScalarField("Status"),
    )

    @classmethod
    def list_from_aws(
        cls: Type["CommandInvocationResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'invocation_1_arn': {invocation_1_dict},
             'invocation_2_arn': {invocation_2_dict},
             ...}

        Where the dicts represent results from list_command_invocations."""
        invocations = {}
        invoked_after = datetime.now(tz=timezone.utc) - timedelta(
            minutes=SSM_COMMAND_INVOCATION_LOOKBACK_MIN
        )
        paginator = client.get_paginator("list_command_invocations")
        for resp in paginator.paginate(
            Filters=[
                {"key": "InvokedAfter", "value": invoked_after.strftime("%Y-%m-%dT%H:%M:%SZ"),}
            ]
        ):
            for invocation in resp.get("CommandInvocations", []):
                resource_arn = cls.generate_arn(
                    account_id=account_id, region=region, resource_id=invocation["CommandId"]
                )
                invocations[resource_arn] = invocation
        return ListFromAWSResult(resources=invocations)
