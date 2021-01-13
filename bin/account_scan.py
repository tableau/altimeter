"""Scan a set of accounts as defined by an AccountScanPlan"""
import json
import logging
from typing import Any, Dict, Tuple

from altimeter.core.artifact_io.writer import ArtifactWriter
from altimeter.aws.scan.account_scanner import AccountScanner
from altimeter.aws.scan.muxer.lambda_muxer import AccountScanLambdaEvent


def lambda_handler(event: Dict[str, Any], _: Any) -> Dict[str, Any]:
    """Entrypoint"""
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)

    account_scan_input = AccountScanLambdaEvent(**event)

    artifact_writer = ArtifactWriter.from_artifact_path(
        artifact_path=account_scan_input.artifact_path, scan_id=account_scan_input.scan_id
    )
    account_scanner = AccountScanner(
        account_scan_plan=account_scan_input.account_scan_plan,
        artifact_writer=artifact_writer,
        max_svc_scan_threads=account_scan_input.max_svc_scan_threads,
        preferred_account_scan_regions=account_scan_input.preferred_account_scan_regions,
        scan_sub_accounts=account_scan_input.scan_sub_accounts,
    )
    scan_results = account_scanner.scan()
    return scan_results.dict()
