#!/usr/bin/env python3
import json

from altimeter.core.artifact_io.writer import S3ArtifactWriter
from altimeter.aws.scan.account_scanner import AccountScanner
from altimeter.aws.scan.account_scan_plan import AccountScanPlan
from altimeter.core.json_encoder import json_encoder
from altimeter.core.awslambda import get_required_lambda_event_var


def lambda_handler(event, context):
    account_scan_plan_dict = get_required_lambda_event_var(event, "account_scan_plan")
    account_scan_plan = AccountScanPlan.from_dict(account_scan_plan_dict)
    json_bucket = get_required_lambda_event_var(event, "json_bucket")
    key_prefix = get_required_lambda_event_var(event, "key_prefix")
    scan_sub_accounts = get_required_lambda_event_var(event, "scan_sub_accounts")

    artifact_writer = S3ArtifactWriter(bucket=json_bucket, key_prefix=key_prefix)
    account_scanner = AccountScanner(
        account_scan_plan=account_scan_plan,
        artifact_writer=artifact_writer,
        scan_sub_accounts=scan_sub_accounts,
    )
    scan_results_dict = account_scanner.scan()
    scan_results_str = json.dumps(scan_results_dict, default=json_encoder)
    json_results = json.loads(scan_results_str)
    return json_results
