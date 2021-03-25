#!/usr/bin/env python3
"""Graph AWS resource data in Neptune"""
from dataclasses import asdict
import logging
import os
from typing import Any, Dict

from pydantic import ValidationError

from altimeter.aws.aws2n import AWS2NConfig, generate_scan_id, aws2n
from altimeter.aws.scan.account_scanner import AccountScanner
from altimeter.aws.scan.muxer.lambda_muxer import AccountScanLambdaEvent, LambdaAWSScanMuxer
from altimeter.core.artifact_io.writer import ArtifactWriter
from altimeter.core.config import AWSConfig, GraphPrunerConfig
from altimeter.core.pruner import prune_graph


class InvalidLambdaInputException(Exception):
    """Indicates the input to the altimeter lambda is invalid"""


def lambda_handler(event: Dict[str, Any], __: Any) -> Dict[str, Any]:
    """AWS Lambda Handler. Depending on the input event and env vars
    either run the aws2n or account_scan processes"""
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    try:
        aws2n_config = AWS2NConfig()
        config = AWSConfig.from_path(path=aws2n_config.config_path)
        scan_id = generate_scan_id()
        muxer = LambdaAWSScanMuxer(
            scan_id=scan_id,
            config=config,
            account_scan_lambda_name=aws2n_config.account_scan_lambda_name,
            account_scan_lambda_timeout=aws2n_config.account_scan_lambda_timeout,
        )
        aws2n_result = aws2n(scan_id=scan_id, config=config, muxer=muxer, load_neptune=True)
        return asdict(aws2n_result)
    except ValidationError:
        pass
    try:
        account_scan_input = AccountScanLambdaEvent(**event)
        artifact_writer = ArtifactWriter.from_artifact_path(
            artifact_path=account_scan_input.artifact_path, scan_id=account_scan_input.scan_id
        )
        account_scanner = AccountScanner(
            account_scan_plan=account_scan_input.account_scan_plan,
            artifact_writer=artifact_writer,
            max_svc_scan_threads=account_scan_input.max_svc_scan_threads,
            scan_sub_accounts=account_scan_input.scan_sub_accounts,
        )
        scan_results = account_scanner.scan()
        return scan_results.dict()
    except ValidationError:
        pass
    try:
        pruner_config = GraphPrunerConfig()
        prune_results = prune_graph(pruner_config)
        return prune_results.dict()
    except ValidationError:
        pass
    raise InvalidLambdaInputException(f"Invalid lambda input.\nENV: {os.environ}\nEvent: {event}")
