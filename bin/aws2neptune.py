#!/usr/bin/env python3
"""Graph AWS resource data in Neptune"""
from datetime import datetime
import argparse
from dataclasses import dataclass
import logging
import json
import os
import sys
from typing import Any, Dict, List, Optional
import uuid

import boto3

from altimeter.aws.log_events import AWSLogEvents
from altimeter.aws.scan.muxer import AWSScanMuxer
from altimeter.aws.scan.muxer.lambda_muxer import LambdaAWSScanMuxer
from altimeter.aws.scan.muxer.local_muxer import LocalAWSScanMuxer
from altimeter.aws.scan.scan import run_scan
from altimeter.core.artifact_io import parse_s3_uri
from altimeter.core.artifact_io.reader import ArtifactReader
from altimeter.core.artifact_io.writer import ArtifactWriter, GZIP
from altimeter.core.config import Config
from altimeter.core.log import Logger
from altimeter.core.log_events import LogEvent
from altimeter.core.neptune.client import AltimeterNeptuneClient, GraphMetadata, NeptuneEndpoint, \
    AltimeterNeptuneGremlinClient
from altimeter.core.parameters import get_required_str_env_var, get_required_int_env_var


@dataclass(frozen=True)
class AWS2NeptuneResult:
    errors: list = None


def aws2neptune_lpg(scan_id: str, config: Config, muxer: AWSScanMuxer) -> AWS2NeptuneResult:
    """Scan AWS resources to json, convert to RDF and load into Neptune
    if config.neptune is defined"""

    logger = Logger()
    artifact_reader = ArtifactReader.from_artifact_path(config.artifact_path)
    artifact_writer = ArtifactWriter.from_artifact_path(
        artifact_path=config.artifact_path, scan_id=scan_id
    )

    logger.info(
        AWSLogEvents.ScanConfigured,
        config=str(config),
        reader=str(artifact_reader.__class__),
        writer=str(artifact_writer.__class__),
    )

    scan_manifest, graph_set = run_scan(
        muxer=muxer,
        config=config,
        artifact_writer=artifact_writer,
        artifact_reader=artifact_reader,
    )
    logger.info(LogEvent.NeptuneGremlinWriteStart, "Beginning writing Neptune Property Graph")
    graph = graph_set.to_neptune_lpg()
    config = {'server': 'XXX', 'port': 80, 'ssl': False}
    AltimeterNeptuneGremlinClient().write_to_neptune(config, graph)
    logger.info(LogEvent.NeptuneGremlinWriteEnd, "Beginning writing Neptune Property Graph")
    return AWS2NeptuneResult()


def generate_scan_id() -> str:
    """Generate a unique scan id"""
    now = datetime.now()
    scan_date = now.strftime("%Y%m%d")
    scan_time = str(int(now.timestamp()))
    scan_id = "/".join((scan_date, scan_time, str(uuid.uuid4())))
    return scan_id


def lambda_handler(event: Dict[str, Any], context: Any) -> None:
    """AWS Lambda Handler"""
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)

    account_scan_lambda_name = get_required_str_env_var(
        "ACCOUNT_SCAN_LAMBDA_NAME")
    account_scan_lambda_timeout = get_required_int_env_var(
        "ACCOUNT_SCAN_LAMBDA_TIMEOUT")

    config_path = get_required_str_env_var("CONFIG_PATH")
    config = Config.from_path(path=config_path)

    scan_id = generate_scan_id()
    muxer = LambdaAWSScanMuxer(
        scan_id=scan_id,
        config=config,
        account_scan_lambda_name=account_scan_lambda_name,
        account_scan_lambda_timeout=account_scan_lambda_timeout,
    )
    aws2neptune(scan_id=scan_id, config=config, muxer=muxer)


def main(argv: Optional[List[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=str, nargs="?")
    args_ns = parser.parse_args(argv)

    config = args_ns.config
    if config is None:
        config = os.environ.get("CONFIG_PATH")
    if config is None:
        print("config must be provided as a positional arg or env var 'CONFIG_PATH'")
        return 1

    config = Config.from_path(config)
    scan_id = generate_scan_id()
    muxer = LocalAWSScanMuxer(scan_id=scan_id, config=config)
    result = aws2neptune_lpg(scan_id=scan_id, config=config,
                   muxer=muxer)
    print(result.rdf_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
