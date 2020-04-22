#!/usr/bin/env python3
"""Graph AWS resource data in Neptune"""
from datetime import datetime
import argparse
from pathlib import Path
import sys
from typing import List, Optional
import uuid

from altimeter.aws.log_events import AWSLogEvents
from altimeter.aws.scan.muxer.local_muxer import LocalAWSScanMuxer
from altimeter.aws.scan.scan import run_scan
from altimeter.core.artifact_io.reader import ArtifactReader
from altimeter.core.artifact_io.writer import ArtifactWriter
from altimeter.core.config import Config
from altimeter.core.log import Logger


def main(argv: Optional[List[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=Path)
    args_ns = parser.parse_args(argv)

    config = Config.from_file(filepath=args_ns.config)
    now = datetime.now()
    scan_date = now.strftime("%Y%m%d")
    scan_time = str(int(now.timestamp()))
    scan_id = "/".join((scan_date, scan_time, str(uuid.uuid4())))
    artifact_reader = ArtifactReader.from_config(config=config)
    artifact_writer = ArtifactWriter.from_config(config=config, scan_id=scan_id)

    logger = Logger()
    logger.info(
        AWSLogEvents.ScanConfigured,
        config=str(config),
        reader=str(artifact_reader.__class__),
        writer=str(artifact_writer.__class__),
    )

    muxer = LocalAWSScanMuxer(scan_id=scan_id, config=config)

    scan_manifest = run_scan(
        muxer=muxer,
        config=config,
        artifact_writer=artifact_writer,
        artifact_reader=artifact_reader,
    )
    json_filepath = scan_manifest.master_artifact
    artifact_writer.write_json("manifest", scan_manifest.to_dict())
    with logger.bind(json_filepath=json_filepath):
        graph_pkg = artifact_reader.read_graph_pkg(json_filepath)
        rdf_path = artifact_writer.write_graph(name="master", graph_pkg=graph_pkg)
    if config.neptune:
        print(rdf_path)
        raise NotImplementedError("Neptune load not implemented")
    return 0


if __name__ == "__main__":
    sys.exit(main())
