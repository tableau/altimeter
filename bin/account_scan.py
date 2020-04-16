#!/usr/bin/env python3
"""Scan a set of accounts as defined by an AccountScanPlan"""
import json
from pathlib import Path
from typing import Any, Dict

from altimeter.core.artifact_io.writer import ArtifactWriter
from altimeter.aws.scan.account_scanner import AccountScanner
from altimeter.aws.scan.account_scan_plan import AccountScanPlan
from altimeter.core.json_encoder import json_encoder
from altimeter.core.parameters import get_required_lambda_event_var
from altimeter.core.config import Config


def lambda_handler(event: Dict[str, Any], context: Any) -> None:
    account_scan_plan_dict = get_required_lambda_event_var(event, "account_scan_plan")
    account_scan_plan = AccountScanPlan.from_dict(account_scan_plan_dict)
    scan_id = get_required_lambda_event_var(event, "scan_id")

    config = Config.from_file(Path("./conf/lambda.toml"))

    artifact_writer = ArtifactWriter.from_config(config=config, scan_id=scan_id)
    account_scanner = AccountScanner(
        account_scan_plan=account_scan_plan, artifact_writer=artifact_writer, config=config,
    )
    scan_results_dict = account_scanner.scan()
    scan_results_str = json.dumps(scan_results_dict, default=json_encoder)
    json_results = json.loads(scan_results_str)
    return json_results
