#!/usr/bin/env python3
"""ScanAccount StepFunction Lambda
Scan a single AWS account and write output to S3."""
import logging
from typing import Any, Dict, Tuple

from pydantic import BaseModel

from altimeter.aws.auth.accessor import Accessor
from altimeter.aws.resource_service_region_mapping import AWSResourceRegionMappingRepository
from altimeter.aws.scan.account_scanner import AccountScanner
from altimeter.aws.scan.scan_plan import AccountScanPlan
from altimeter.core.artifact_io.writer import ArtifactWriter


class AccountScanInput(BaseModel):
    scan_id: str
    account_id: str
    regions: Tuple[str, ...]
    aws_resource_region_mapping_repo: AWSResourceRegionMappingRepository
    accessor: Accessor
    artifact_path: str
    max_svc_scan_threads: int
    scan_sub_accounts: bool


def lambda_handler(event: Dict[str, Any], _: Any) -> Dict[str, Any]:
    """Lambda entrypoint"""
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    account_scan_input = AccountScanInput(**event)
    artifact_writer = ArtifactWriter.from_artifact_path(
        artifact_path=account_scan_input.artifact_path, scan_id=account_scan_input.scan_id
    )
    account_scan_plan = AccountScanPlan(
        account_id=account_scan_input.account_id,
        regions=account_scan_input.regions,
        aws_resource_region_mapping_repo=account_scan_input.aws_resource_region_mapping_repo,
        accessor=account_scan_input.accessor,
    )
    account_scanner = AccountScanner(
        account_scan_plan=account_scan_plan,
        artifact_writer=artifact_writer,
        max_svc_scan_threads=account_scan_input.max_svc_scan_threads,
        scan_sub_accounts=account_scan_input.scan_sub_accounts,
    )
    scan_results = account_scanner.scan()
    return scan_results.dict()
