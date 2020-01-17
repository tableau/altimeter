"""Resource representing AWS Support."""
from typing import Type

from botocore.client import BaseClient

from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema
from altimeter.aws.resource.resource_spec import ListFromAWSResult
from altimeter.aws.resource.support import SupportResourceSpec


class SeverityLevelResourceSpec(SupportResourceSpec):
    """Resource representing an AWS Support severity level."""

    type_name = "severity-level"
    schema = Schema(ScalarField("code"))

    @classmethod
    def list_from_aws(
        cls: Type["SeverityLevelResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'severity_level_arn': {severity_level_dict},
             'severity_level_arn': {severity_level_dict},
             ...}

        Where the dicts represent results from describe_organization."""
        resp = client.describe_severity_levels()
        severity_levels_resp = resp["severityLevels"]
        severity_levels = {}
        for s_l in severity_levels_resp:
            code = s_l["code"]
            code_arn = cls.generate_arn(resource_id=code, account_id=account_id)
            severity_levels[code_arn] = {"code": code}
        return ListFromAWSResult(resources=severity_levels)
