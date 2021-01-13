"""An AccountScanManifest defines the output of an account scan."""

from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class AccountScanManifest:
    """An AccountScanManifest defines the output of an account scan. It contains pointers to the
    scan result artifacts and summaries of what was scanned and errors which occurred.

    Args:
        account_id: account id
        artifacts: list of scan artifacts
        errors: list of error strings
    """

    account_id: str
    artifacts: List[str]
    errors: List[str]
