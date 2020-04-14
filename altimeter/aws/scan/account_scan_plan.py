"""An AccountScanPlan defines how to scan a set of accounts."""
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Type

from altimeter.aws.auth.accessor import Accessor


@dataclass(frozen=True)
class AccountScanPlan:
    """An AccountScanPlan defines how to scan a set of accounts.

    Arguments:
        account_ids: account ids to scan
        regions: regions to scan
        accessor: Accessor to use to access the accounts
    """

    account_ids: Tuple[str, ...]
    regions: Tuple[str, ...]
    accessor: Accessor

    def to_dict(self) -> Dict[str, Any]:
        """Generate a dict representation of this AccountScanPlan.

        Returns:
            dict representation of this AccountScanPlan
        """
        return {
            "account_ids": self.account_ids,
            "regions": list(self.regions),
            "accessor": self.accessor.to_dict(),
        }

    @classmethod
    def from_dict(
        cls: Type["AccountScanPlan"], account_scan_plan_dict: Dict[str, Any]
    ) -> "AccountScanPlan":
        """Create an AccountScanPlan from a dict

        Args:
           account_scan_plan_dict: dict of AccountScanPlan data

        Returns:
            AccountScanPlan object
        """
        account_ids = account_scan_plan_dict["account_ids"]
        regions = account_scan_plan_dict["regions"]
        accessor_dict = account_scan_plan_dict["accessor"]
        accessor = Accessor.from_dict(accessor_dict)
        return cls(account_ids=account_ids, regions=regions, accessor=accessor)

    def to_batches(self, max_accounts: int) -> List["AccountScanPlan"]:
        """Break this AccountScanPlan into multiple AccountScanPlans with a max of
        max_accounts account_ids per plan"""
        account_id_batches = [
            self.account_ids[n : n + max_accounts]
            for n in range(0, len(self.account_ids), max_accounts)
        ]
        return [
            AccountScanPlan(
                account_ids=account_id_batch, regions=self.regions, accessor=self.accessor
            )
            for account_id_batch in account_id_batches
        ]
