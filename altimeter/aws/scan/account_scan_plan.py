"""An AccountScanPlan defines how to scan an account."""
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type

import boto3

from altimeter.aws.access.accessor import Accessor


@dataclass(frozen=True)
class AccountScanPlan:
    """An AccountScanPlan defines how to scan an account.

    Arguments:
        account_id: account id to scan
        regions: regions to scan
        accessor: Accessor to use to access the account
    """

    account_id: str
    regions: List[str]
    accessor: Accessor

    def get_session(self, region: Optional[str] = None) -> boto3.Session:
        """Get a boto3 Session for this AccountScanPlan's account_id
        Args:
             region: specific region to acquire the session in
        Returns:
            session object
        """
        return self.accessor.get_session(account_id=self.account_id, region=region)

    def to_dict(self) -> Dict[str, Any]:
        """Generate a dict representation of this AccountScanPlan.

        Returns:
            dict representation of this AccountScanPlan
        """
        return {
            "account_id": self.account_id,
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
        account_id = account_scan_plan_dict["account_id"]
        regions = account_scan_plan_dict["regions"]
        accessor_dict = account_scan_plan_dict["accessor"]
        accessor = Accessor.from_dict(accessor_dict)
        return cls(account_id=account_id, regions=regions, accessor=accessor)


def build_account_scan_plans(
    accessor: Accessor, account_ids: List[str], regions: List[str]
) -> List[AccountScanPlan]:
    """Given a list of account_ids and regions build a list of AccountScanPlans.

    Args:
        accessor: Accessor to use for each AccountScanPlan
        account_ids: list of account ids to build plans for
        regions: regions to scan

    Returns:
        List of AccountScanPlan objects.
    """
    account_scan_plans: List[AccountScanPlan] = []
    # first any free-floating accounts
    for account_id in account_ids:
        account_scan_plan = AccountScanPlan(
            account_id=account_id, regions=regions, accessor=accessor
        )
        account_scan_plans.append(account_scan_plan)
    return account_scan_plans
