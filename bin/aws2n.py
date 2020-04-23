#!/usr/bin/env python3
"""Graph AWS resource data in Neptune"""
from datetime import datetime
import argparse
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional
import uuid

from altimeter.aws.log_events import AWSLogEvents
from altimeter.aws.scan.muxer import AWSScanMuxer
from altimeter.aws.scan.muxer.lambda_muxer import LambdaAWSScanMuxer
from altimeter.aws.scan.muxer.local_muxer import LocalAWSScanMuxer
from altimeter.aws.scan.scan import run_scan
from altimeter.core.artifact_io.reader import ArtifactReader
from altimeter.core.artifact_io.writer import ArtifactWriter
from altimeter.core.config import Config
from altimeter.core.log import Logger
from altimeter.core.parameters import get_required_str_env_var, get_required_int_env_var

@dataclass(frozen=True)
class AWS2NResult:
    json_path: str
    rdf_path: str

def aws2n(scan_id: str, config: Config, muxer: AWSScanMuxer) -> AWS2NResult:
    artifact_reader = ArtifactReader.from_config(config=config)
    artifact_writer = ArtifactWriter.from_config(config=config, scan_id=scan_id)

    logger = Logger()
    logger.info(
        AWSLogEvents.ScanConfigured,
        config=str(config),
        reader=str(artifact_reader.__class__),
        writer=str(artifact_writer.__class__),
    )

    scan_manifest = run_scan(
        muxer=muxer,
        config=config,
        artifact_writer=artifact_writer,
        artifact_reader=artifact_reader,
    )
    json_path = scan_manifest.master_artifact
    artifact_writer.write_json("manifest", scan_manifest.to_dict())
    with logger.bind(json_path=json_path):
        graph_pkg = artifact_reader.read_graph_pkg(json_path)
        rdf_path = artifact_writer.write_graph(name="master", graph_pkg=graph_pkg)
    if config.neptune:
        raise NotImplementedError(f"Neptune load not implemented ; {rdf_path}")
    return AWS2NResult(json_path=json_path,
                       rdf_path=rdf_path)


def generate_scan_id() -> str:
    """Generate a unique scan id"""
    now = datetime.now()
    scan_date = now.strftime("%Y%m%d")
    scan_time = str(int(now.timestamp()))
    scan_id = "/".join((scan_date, scan_time, str(uuid.uuid4())))
    return scan_id


def lambda_handler(event: Dict[str, Any], context: Any) -> None:
    """AWS Lambda Handler"""
    account_scan_lambda_name = get_required_str_env_var("ACCOUNT_SCAN_LAMBDA_NAME")
    account_scan_lambda_timeout = get_required_int_env_var("ACCOUNT_SCAN_LAMBDA_TIMEOUT")

    config = Config.from_file(Path("./conf/lambda.toml"))
    scan_id = generate_scan_id()
    muxer = LambdaAWSScanMuxer(
        scan_id=scan_id,
        config=config,
        account_scan_lambda_name=account_scan_lambda_name,
        account_scan_lambda_timeout=account_scan_lambda_timeout,
    )
    aws2n(scan_id=scan_id, config=config, muxer=muxer)


def main(argv: Optional[List[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=Path)
    args_ns = parser.parse_args(argv)

    config = Config.from_file(filepath=args_ns.config)
    scan_id = generate_scan_id()
    muxer = LocalAWSScanMuxer(scan_id=scan_id, config=config)
    result = aws2n(scan_id=scan_id, config=config, muxer=muxer)
    print(result.rdf_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
