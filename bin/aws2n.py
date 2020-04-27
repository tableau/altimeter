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
from altimeter.core.neptune.client import AltimeterNeptuneClient, GraphMetadata, NeptuneEndpoint
from altimeter.core.parameters import get_required_str_env_var, get_required_int_env_var


@dataclass(frozen=True)
class AWS2NResult:
    json_path: str
    rdf_path: str
    graph_metadata: Optional[GraphMetadata]


def aws2n(scan_id: str, config: Config, muxer: AWSScanMuxer, load_neptune: bool) -> AWS2NResult:
    """Scan AWS resources to json, convert to RDF and load into Neptune
    if config.neptune is defined"""
    artifact_reader = ArtifactReader.from_artifact_path(config.artifact_path)
    artifact_writer = ArtifactWriter.from_artifact_path(
        artifact_path=config.artifact_path, scan_id=scan_id
    )

    logger = Logger()
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
    json_path = scan_manifest.master_artifact
    rdf_path = artifact_writer.write_graph_set(name="master", graph_set=graph_set, compression=GZIP)
    graph_metadata = None
    if load_neptune:
        if config.neptune is None:
            raise Exception("Can not load to Neptune because config.neptune is empty.")
        endpoint = NeptuneEndpoint(
            host=config.neptune.host, port=config.neptune.port, region=config.neptune.region
        )
        neptune_client = AltimeterNeptuneClient(max_age_min=1440, neptune_endpoint=endpoint)
        rdf_bucket, rdf_key = parse_s3_uri(rdf_path)
        if rdf_key is None:
            raise Exception(f"Invalid rdf s3 path {rdf_path}")
        graph_metadata = neptune_client.load_graph(
            bucket=rdf_bucket, key=rdf_key, load_iam_role_arn=config.neptune.iam_role_arn
        )
        logger.info(event=LogEvent.GraphLoadedSNSNotificationStart)
        sns_client = boto3.client("sns")
        message_dict = {
            "uri": graph_metadata.uri,
            "name": graph_metadata.name,
            "version": graph_metadata.version,
            "start_time": graph_metadata.start_time,
            "end_time": graph_metadata.end_time,
            "neptune_endpoint": endpoint.get_endpoint_str(),
        }
        message_dict["default"] = json.dumps(message_dict)
        sns_client.publish(
            TopicArn=config.neptune.graph_load_sns_topic_arn,
            MessageStructure="json",
            Message=json.dumps(message_dict),
        )
        logger.info(event=LogEvent.GraphLoadedSNSNotificationEnd)
    return AWS2NResult(json_path=json_path, rdf_path=rdf_path, graph_metadata=graph_metadata)


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

    account_scan_lambda_name = get_required_str_env_var("ACCOUNT_SCAN_LAMBDA_NAME")
    account_scan_lambda_timeout = get_required_int_env_var("ACCOUNT_SCAN_LAMBDA_TIMEOUT")

    config_path = get_required_str_env_var("CONFIG_PATH")
    config = Config.from_path(path=config_path)

    scan_id = generate_scan_id()
    muxer = LambdaAWSScanMuxer(
        scan_id=scan_id,
        config=config,
        account_scan_lambda_name=account_scan_lambda_name,
        account_scan_lambda_timeout=account_scan_lambda_timeout,
    )
    aws2n(scan_id=scan_id, config=config, muxer=muxer, load_neptune=True)


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
    result = aws2n(scan_id=scan_id, config=config, muxer=muxer, load_neptune=False)
    print(result.rdf_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
