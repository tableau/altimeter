#!/usr/bin/env python3
"""Pull data from AWS and convert it to JSON for consumption by json2rdf.py"""
from datetime import datetime
from pathlib import Path
from typing import Any, Dict
import uuid

from altimeter.aws.scan.scan import run_scan
from altimeter.core.parameters import get_required_str_env_var, get_required_int_env_var
from altimeter.core.config import Config
from altimeter.core.log import Logger
from altimeter.aws.log_events import AWSLogEvents

from altimeter.core.artifact_io.reader import ArtifactReader
from altimeter.core.artifact_io.writer import ArtifactWriter

from altimeter.aws.scan.muxer.lambda_muxer import LambdaAWSScanMuxer


def lambda_handler(event: Dict[str, Any], context: Any) -> None:
    """AWS Lambda Handler"""
    account_scan_lambda_name = get_required_str_env_var("ACCOUNT_SCAN_LAMBDA_NAME")
    account_scan_lambda_timeout = get_required_int_env_var("ACCOUNT_SCAN_LAMBDA_TIMEOUT")

    config = Config.from_file(Path("./conf/lambda.toml"))

    logger = Logger()
    logger.info(
        AWSLogEvents.ScanConfigured,
        config=str(config),
        account_scan_lambda_name=account_scan_lambda_name,
        account_scan_lambda_timeout=account_scan_lambda_timeout,
    )

    now = datetime.now()
    scan_date = now.strftime("%Y%m%d")
    scan_time = str(int(now.timestamp()))
    scan_id = "/".join((scan_date, scan_time, str(uuid.uuid4())))
    artifact_reader = ArtifactReader.from_config(config=config)
    artifact_writer = ArtifactWriter.from_config(config=config, scan_id=scan_id)

    muxer = LambdaAWSScanMuxer(
        scan_id=scan_id,
        config=config,
        account_scan_lambda_name=account_scan_lambda_name,
        account_scan_lambda_timeout=account_scan_lambda_timeout,
    )

    scan_manifest = run_scan(
        muxer=muxer,
        config=config,
        artifact_writer=artifact_writer,
        artifact_reader=artifact_reader,
    )

    artifact_writer.write_json("manifest", scan_manifest.to_dict())
