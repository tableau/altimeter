"""A ScanManifest defines the output of a complete scan."""

from typing import Dict, List, Optional

from altimeter.core.base_model import BaseImmutableModel


class ScanManifest(BaseImmutableModel):
    """A ScanManifest defines the output of a complete scan. It contains pointers to the
    per-account scan result artifacts and summaries of what was scanned, errors which occurred,
    scan datetime and api call statistics.

    Args:
        scanned_accounts: List of account ids which were scanned
        master_artifact: artifact containing complete graph json
        artifacts: list of artifacts, one per account
        errors: Dict of account_ids to list of errors encountered during scan
        unscanned_accounts: List of account ids which were not scanned
        start_time: epoch timestamp of scan start time
        end_time: epoch timestamp of scan end time
    """

    scanned_accounts: List[str]
    master_artifact: Optional[str] = None
    artifacts: List[str]
    errors: Dict[str, List[str]]
    unscanned_accounts: List[str]
    start_time: int
    end_time: int
