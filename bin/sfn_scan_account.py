#!/usr/bin/env python3
"""ScanAccount StepFunction Lambda
Scan a single AWS account and write output to S3."""
import logging
from typing import Any, Dict

from pydantic import BaseModel

from altimeter.aws.resource_service_region_mapping import AWSResourceRegionMappingRepository
from altimeter.aws.scan.account_scanner import AccountScanner
from altimeter.aws.scan.scan_plan import AccountScanPlan
from altimeter.aws.scan.settings import DEFAULT_RESOURCE_SPEC_CLASSES
from altimeter.core.artifact_io.writer import ArtifactWriter
from altimeter.core.config import AWSConfig


class AccountScanInput(BaseModel):
    config: AWSConfig
    scan_id: str
    account_id: str
    aws_resource_region_mapping_repo: AWSResourceRegionMappingRepository


def lambda_handler(event: Dict[str, Any], _: Any) -> Dict[str, Any]:
    """Lambda entrypoint"""
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    account_scan_input = AccountScanInput(**event)
    artifact_writer = ArtifactWriter.from_artifact_path(
        artifact_path=account_scan_input.config.artifact_path, scan_id=account_scan_input.scan_id
    )
    account_scan_plan = AccountScanPlan(
        account_id=account_scan_input.account_id,
        regions=account_scan_input.config.scan.regions,
        aws_resource_region_mapping_repo=account_scan_input.aws_resource_region_mapping_repo,
        accessor=account_scan_input.config.accessor,
    )
    account_scanner = AccountScanner(
        account_scan_plan=account_scan_plan,
        artifact_writer=artifact_writer,
        max_svc_scan_threads=account_scan_input.config.concurrency.max_svc_scan_threads,
        scan_sub_accounts=account_scan_input.config.scan.scan_sub_accounts,
        resource_spec_classes=DEFAULT_RESOURCE_SPEC_CLASSES,
    )
    scan_results = account_scanner.scan()
    return scan_results.dict()
