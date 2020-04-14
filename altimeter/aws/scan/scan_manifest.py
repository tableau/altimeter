"""A ScanManifest defines the output of a complete scan."""

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass(frozen=True)
class ScanManifest:
    """A ScanManifest defines the output of a complete scan. It contains pointers to the
    per-account scan result artifacts and summaries of what was scanned, errors which occurred,
    scan datetime and api call statistics.

    Args:
        scanned_accounts: List of account ids which were scanned
        master_artifact: artifact containing complete graph json
        artifacts: list of artifacts, one per account
        errors: Dict of account_ids to list of errors encountered during scan
        unscanned_accounts: List of account ids which were not scanned
        api_call_stats: api call stats for this scan
        start_time: epoch timestamp of scan start time
        end_time: epoch timestamp of scan end time
    """

    scanned_accounts: List[str]
    master_artifact: str
    artifacts: List[str]
    errors: Dict[str, List[str]]
    unscanned_accounts: List[str]
    api_call_stats: Dict[str, Any]
    start_time: int
    end_time: int

    def to_dict(self) -> Dict[str, Any]:
        """Generate a dict representation of this ScanManifest.

        Returns:
            dict representation of this ScanManifest
        """
        return {
            "scanned_accounts": self.scanned_accounts,
            "master_artifact": self.master_artifact,
            "artifacts": self.artifacts,
            "errors": self.errors,
            "unscanned_accounts": self.unscanned_accounts,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "api_call_stats": self.api_call_stats,
        }
