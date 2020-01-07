"""Resource representing an AWS Account"""
from typing import List, Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ScanGranularity, ListFromAWSResult, AWSResourceSpec
from altimeter.aws.resource.unscanned_account import UnscannedAccountResourceSpec
from altimeter.core.resource.resource_spec import ResourceSpec
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class AccountResourceSpec(AWSResourceSpec):
    """Resource representing an AWS Account"""

    type_name = "account"
    service_name = "sts"
    scan_granularity = ScanGranularity.ACCOUNT
    schema = Schema(ScalarField("account_id"))
    allow_clobber: List[Type[ResourceSpec]] = [UnscannedAccountResourceSpec]

    @classmethod
    def get_full_type_name(cls: Type["AccountResourceSpec"]) -> str:
        return f"{cls.provider_name}:{cls.type_name}"

    @classmethod
    def list_from_aws(
        cls: Type["AccountResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """This resource is somewhat synthetic, this method simply returns a dict of form
            {'account_arn': {account_dict}"""
        sts_account_id = client.get_caller_identity()["Account"]
        if sts_account_id != account_id:
            raise ValueError(f"BUG: sts detected account_id {sts_account_id} != {account_id}")
        accounts = {f"arn:aws::::account/{sts_account_id}": {"account_id": sts_account_id}}
        return ListFromAWSResult(resources=accounts)

    @classmethod
    def generate_arn(
        cls: Type["AccountResourceSpec"], resource_id: str, account_id: str = "", region: str = "",
    ) -> str:
        """Generate an ARN for this resource"""
        return f"arn:aws::::account/{resource_id}"
