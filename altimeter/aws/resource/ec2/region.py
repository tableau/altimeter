"""Resource representing an AWS Region"""
from typing import Type

from botocore.client import BaseClient

from altimeter.aws.resource.resource_spec import ScanGranularity, ListFromAWSResult
from altimeter.aws.resource.ec2 import EC2ResourceSpec
from altimeter.core.graph.field.scalar_field import ScalarField
from altimeter.core.graph.schema import Schema


class RegionResourceSpec(EC2ResourceSpec):
    """Resource representing an AWS Region"""

    type_name = "region"
    scan_granularity = ScanGranularity.ACCOUNT
    schema = Schema(ScalarField("RegionName", "name"), ScalarField("OptInStatus"))

    @classmethod
    def get_full_type_name(cls: Type["RegionResourceSpec"]) -> str:
        return f"{cls.provider_name}:{cls.type_name}"

    @classmethod
    def list_from_aws(
        cls: Type["RegionResourceSpec"], client: BaseClient, account_id: str, region: str
    ) -> ListFromAWSResult:
        """Return a dict of dicts of the format:

            {'region_1_arn': {region_1_dict},
             'region_2_arn': {region_2_dict},
             ...}

        Where the dicts represent results from describe_regions."""
        regions = {}
        resp = client.describe_regions(AllRegions=True)
        for region_resp in resp["Regions"]:
            region_name = region_resp["RegionName"]
            region_arn = f"arn:aws:::{account_id}:region/{region_name}"
            regions[region_arn] = region_resp
        return ListFromAWSResult(resources=regions)
