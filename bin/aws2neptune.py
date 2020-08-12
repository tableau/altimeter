#!/usr/bin/env python3
"""Graph AWS resource data in Neptune"""
from datetime import datetime
import sys
import os
from typing import List, Optional
import uuid
import boto3

from distutils.util import strtobool
from altimeter.aws.log_events import AWSLogEvents
from altimeter.aws.scan.muxer import AWSScanMuxer
from altimeter.aws.scan.muxer.local_muxer import LocalAWSScanMuxer
from altimeter.aws.scan.scan import run_scan
from altimeter.core.artifact_io.reader import ArtifactReader
from altimeter.core.artifact_io.writer import ArtifactWriter
from altimeter.core.config import Config
from altimeter.core.log import Logger
from altimeter.core.log_events import LogEvent
from altimeter.core.neptune.client import AltimeterNeptuneClient, NeptuneEndpoint


def aws2neptune_lpg(scan_id: str, config: Config, muxer: AWSScanMuxer) -> None:
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
    logger.info(LogEvent.NeptuneGremlinWriteStart)
    graph = graph_set.to_neptune_lpg(scan_id)
    if config.neptune is None:
        raise Exception("Can not load to Neptune because config.neptune is empty.")
    endpoint = NeptuneEndpoint(
        host=config.neptune.host,
        port=config.neptune.port,
        region=config.neptune.region,
        ssl=bool(config.neptune.ssl),
    )
    neptune_client = AltimeterNeptuneClient(max_age_min=1440, neptune_endpoint=endpoint)
    neptune_client.write_to_neptune_lpg(graph, scan_id)
    logger.info(LogEvent.NeptuneGremlinWriteEnd)


def aws2neptune_rdf(scan_id: str, config: Config, muxer: AWSScanMuxer) -> None:
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
    logger.info(LogEvent.NeptuneRDFWriteStart)
    graph = graph_set.to_rdf()
    if config.neptune is None:
        raise Exception("Can not load to Neptune because config.neptune is empty.")
    endpoint = NeptuneEndpoint(
        host=config.neptune.host,
        port=config.neptune.port,
        region=config.neptune.region,
        ssl=bool(config.neptune.ssl),
    )
    neptune_client = AltimeterNeptuneClient(max_age_min=1440, neptune_endpoint=endpoint)
    neptune_client.write_to_neptune_rdf(graph)
    logger.info(LogEvent.NeptuneRDFWriteEnd)


def generate_scan_id() -> str:
    """Generate a unique scan id"""
    now = datetime.now()
    scan_date = now.strftime("%Y%m%d")
    scan_time = str(int(now.timestamp()))
    scan_id = "/".join((scan_date, scan_time, str(uuid.uuid4())))
    return scan_id


def main(argv: Optional[List[str]] = None) -> int:
    current_account = boto3.client("sts").get_caller_identity().get("Account")
    current_region = boto3.session.Session().region_name
    config_dict = {
        "artifact_path": "./altimeter",
        "pruner_max_age_min": 4320,
        "graph_name": "alti",
        "access": {"cache_creds": True},
        "concurrency": {
            "max_account_scan_threads": 1,
            "max_accounts_per_thread": 1,
            "max_svc_scan_threads": 64,
        },
        "scan": {
            "accounts": [current_account],
            "regions": [current_region],
            "scan_sub_accounts": False,
            "preferred_account_scan_regions": [current_region],
        },
        "neptune": {
            "host": os.environ["GRAPH_NOTEBOOK_HOST"],
            "port": int(os.environ["GRAPH_NOTEBOOK_PORT"]),
            "region": os.environ["AWS_REGION"],
            "ssl": bool(strtobool(os.environ["GRAPH_NOTEBOOK_SSL"])),
            "use_lpg": True if os.environ["ALTIMETER_DATA_MODEL"] == "LPG" else False,
        },
    }

    config = Config.from_dict(config_dict)
    scan_id = generate_scan_id()
    muxer = LocalAWSScanMuxer(scan_id=scan_id, config=config)

    if config.neptune:
        if bool(config.neptune.use_lpg):
            aws2neptune_lpg(scan_id=scan_id, config=config, muxer=muxer)
        else:
            aws2neptune_rdf(scan_id=scan_id, config=config, muxer=muxer)
    else:
        raise Exception("Can not load to Neptune because config.neptune is empty.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
