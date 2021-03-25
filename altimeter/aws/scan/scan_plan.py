"""A ScanPlan defines how to scan a set of accounts."""
from typing import Optional, Tuple

from altimeter.aws.auth.accessor import Accessor
from altimeter.aws.resource_service_region_mapping import AWSResourceRegionMappingRepository
from altimeter.core.base_model import BaseImmutableModel


class AccountScanPlan(BaseImmutableModel):
    """An AccountScanPlan defines how to scan an account.

    Arguments:
        account_id: account id to scan
        regions: regions to scan
        aws_resource_region_mapping_repo: resource/region mapping
        accessor: Accessor to use to access the accounts
    """

    account_id: str
    regions: Tuple[str, ...]
    aws_resource_region_mapping_repo: AWSResourceRegionMappingRepository
    accessor: Accessor


class ScanPlan(BaseImmutableModel):
    """A ScanPlan defines how to scan a set of accounts.

    Arguments:
        account_ids: account ids to scan
        regions: regions to scan
        aws_resource_region_mapping_repo: resource/region mapping
        accessor: Accessor to use to access the accounts
    """

    account_ids: Tuple[str, ...]
    regions: Tuple[str, ...]
    aws_resource_region_mapping_repo: AWSResourceRegionMappingRepository
    accessor: Accessor

    def build_account_scan_plans(
        self, account_id_blacklist: Optional[Tuple[str, ...]] = None
    ) -> Tuple[AccountScanPlan, ...]:
        if account_id_blacklist is None:
            account_id_blacklist = tuple()
        return tuple(
            [
                AccountScanPlan(
                    account_id=account_id,
                    regions=self.regions,
                    aws_resource_region_mapping_repo=self.aws_resource_region_mapping_repo,
                    accessor=self.accessor,
                )
                for account_id in self.account_ids
                if account_id not in account_id_blacklist
            ]
        )
