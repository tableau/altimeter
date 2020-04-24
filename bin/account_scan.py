"""Scan a set of accounts as defined by an AccountScanPlan"""
import json
import logging
from typing import Any, Dict

from altimeter.core.artifact_io.writer import ArtifactWriter
from altimeter.aws.scan.account_scanner import AccountScanner
from altimeter.aws.scan.account_scan_plan import AccountScanPlan
from altimeter.core.json_encoder import json_encoder
from altimeter.core.parameters import get_required_lambda_event_var


def lambda_handler(event: Dict[str, Any], context: Any) -> None:
    """Entrypoint"""
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)

    account_scan_plan_dict = get_required_lambda_event_var(event, "account_scan_plan")
    account_scan_plan = AccountScanPlan.from_dict(account_scan_plan_dict)
    scan_id = get_required_lambda_event_var(event, "scan_id")
    artifact_path = get_required_lambda_event_var(event, "artifact_path")
    max_svc_scan_threads = get_required_lambda_event_var(event, "max_svc_scan_threads")
    preferred_account_scan_regions = get_required_lambda_event_var(
        event, "preferred_account_scan_regions"
    )
    scan_sub_accounts = get_required_lambda_event_var(event, "scan_sub_accounts")

    artifact_writer = ArtifactWriter.from_artifact_path(
        artifact_path=artifact_path, scan_id=scan_id
    )
    account_scanner = AccountScanner(
        account_scan_plan=account_scan_plan,
        artifact_writer=artifact_writer,
        max_svc_scan_threads=max_svc_scan_threads,
        preferred_account_scan_regions=preferred_account_scan_regions,
        scan_sub_accounts=scan_sub_accounts,
    )
    scan_results_dict = account_scanner.scan()
    scan_results_str = json.dumps(scan_results_dict, default=json_encoder)
    json_results = json.loads(scan_results_str)
    return json_results
