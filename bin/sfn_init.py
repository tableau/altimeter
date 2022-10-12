#!/usr/bin/env python3
"""Initialization StepFunction Lambda
This is the first step of Altimeter as a step function. This reads the Altimeter config and outputs an InitOutput
which contains the data required for subsequent AccountScans via sfn_scan_account."""
import logging
from typing import Any, Dict, Tuple

import boto3
from pydantic import BaseSettings

from altimeter.aws.auth.accessor import Accessor
from altimeter.aws.aws2n import generate_scan_id
from altimeter.aws.resource_service_region_mapping import (
    build_aws_resource_region_mapping_repo,
    AWSResourceRegionMappingRepository,
)
from altimeter.aws.scan.scan import get_sub_account_ids
from altimeter.core.base_model import BaseImmutableModel
from altimeter.core.config import AWSConfig


class Settings(BaseSettings):
    config_path: str


class InitOutput(BaseImmutableModel):
    scan_id: str
    account_ids: Tuple[str, ...]
    regions: Tuple[str, ...]
    aws_resource_region_mapping_repo: AWSResourceRegionMappingRepository
    accessor: Accessor
    artifact_path: str
    max_svc_scan_threads: int
    scan_sub_accounts: bool


def lambda_handler(_: Dict[str, Any], __: Any) -> Dict[str, Any]:
    """Lambda entrypoint"""
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    settings = Settings()
    config = AWSConfig.from_path(path=settings.config_path)
    config.accessor.cache_creds = False
    scan_id = generate_scan_id()
    aws_resource_region_mapping_repo = build_aws_resource_region_mapping_repo(
        global_region_whitelist=config.scan.regions,
        preferred_account_scan_regions=config.scan.preferred_account_scan_regions,
        services_regions_json_url=config.services_regions_json_url,
    )
    if config.scan.accounts:
        scan_account_ids = config.scan.accounts
    else:
        sts_client = boto3.client("sts")
        scan_account_id = sts_client.get_caller_identity()["Account"]
        scan_account_ids = (scan_account_id,)
    if config.scan.scan_sub_accounts:
        account_ids = get_sub_account_ids(scan_account_ids, config.accessor)
    else:
        account_ids = scan_account_ids
    return InitOutput(
        scan_id=scan_id,
        account_ids=account_ids,
        regions=config.scan.regions,
        aws_resource_region_mapping_repo=aws_resource_region_mapping_repo,
        accessor=config.accessor,
        artifact_path=config.artifact_path,
        max_svc_scan_threads=config.concurrency.max_svc_scan_threads,
        scan_sub_accounts=config.scan.scan_sub_accounts,
    ).dict()
