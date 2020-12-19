"""An AccountScanManifest defines the output of an account scan."""

from dataclasses import dataclass
from typing import List

from altimeter.core.multilevel_counter import MultilevelCounter


@dataclass(frozen=True)
class AccountScanManifest:
    """An AccountScanManifest defines the output of an account scan. It contains pointers to the
    scan result artifacts and summaries of what was scanned, errors which occurred, and api call
    statistics.

    Args:
        account_id: account id
        artifacts: list of scan artifacts
        errors: list of error strings
        api_call_stats: api call stats
    """

    account_id: str
    artifacts: List[str]
    errors: List[str]
    api_call_stats: MultilevelCounter
