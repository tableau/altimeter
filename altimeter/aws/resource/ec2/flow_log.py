"""Resource for VPC Flow Logs"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.ec2 import EC2ResourceSpec
from altimeter.aws.resource.ec2.vpc import VPCResourceSpec
from altimeter.core.graph.field.resource_link_field import TransientResourceLinkField
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class FlowLogResourceSpec(EC2ResourceSpec):
    """Resource for VPC Flow Logs"""

    type_name = "flow-log"
    schema = Schema(
        ScalarField("CreationTime"),
        ScalarField("DeliverLogsErrorMessage", optional=True),
        ScalarField("DeliverLogsPermissionArn", optional=True),
        ScalarField("DeliverLogsStatus", optional=True),
        ScalarField("FlowLogStatus"),
        ScalarField("LogGroupName", optional=True),
        TransientResourceLinkField("ResourceId", VPCResourceSpec, optional=True),
        ScalarField("TrafficType"),
        ScalarField("LogDestinationType"),
        ScalarField("LogDestination", optional=True),
        ScalarField("LogFormat"),
    )

    @classmethod
    def list_from_aws(
        cls: Type["FlowLogResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'fl_1_arn': {fl_1_dict},
             'fl_2_arn': {fl_2_dict},
             ...}

        Where the dicts represent results from describe_flow_logs."""
        flow_logs = {}
        paginator = client.get_paginator("describe_flow_logs")
        for resp in paginator.paginate():
            for flow_log in resp.get("FlowLogs", []):
                resource_arn = cls.generate_arn(
                    account_id=account_id, region=region, resource_id=flow_log["FlowLogId"]
                )
                flow_logs[resource_arn] = flow_log
        return ListFromAWSResult(resources=flow_logs)
