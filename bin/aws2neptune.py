#!/usr/bin/env python3
"""Graph AWS resource data in Neptune"""
from datetime import datetime
import sys
from typing import List, Optional, Type
import uuid
import argparse
import json
import boto3

from altimeter.aws.log_events import AWSLogEvents
from altimeter.aws.resource.resource_spec import AWSResourceSpec
from altimeter.aws.resource_service_region_mapping import build_aws_resource_region_mapping_repo
from altimeter.aws.scan.muxer import AWSScanMuxer
from altimeter.aws.scan.muxer.local_muxer import LocalAWSScanMuxer
from altimeter.aws.scan.scan import run_scan
from altimeter.aws.scan.settings import DEFAULT_RESOURCE_SPEC_CLASSES
from altimeter.core.artifact_io.reader import ArtifactReader
from altimeter.core.artifact_io.writer import ArtifactWriter
from altimeter.core.config import ConcurrencyConfig, AWSConfig, NeptuneConfig, ScanConfig
from altimeter.core.log import Logger
from altimeter.core.log_events import LogEvent
from altimeter.core.neptune.client import AltimeterNeptuneClient, NeptuneEndpoint

logger = Logger(pretty_output=True)


def aws2neptune_lpg(scan_id: str, config: AWSConfig, muxer: AWSScanMuxer) -> None:
    """Scan AWS resources to json, convert to RDF and load into Neptune
    if config.neptune is defined"""

    artifact_reader = ArtifactReader.from_artifact_path(config.artifact_path)
    artifact_writer = ArtifactWriter.from_artifact_path(
        artifact_path=config.artifact_path, scan_id=scan_id
    )

    aws_resource_region_mapping_repo = build_aws_resource_region_mapping_repo(
        global_region_whitelist=config.scan.regions,
        preferred_account_scan_regions=config.scan.preferred_account_scan_regions,
        services_regions_json_url=config.services_regions_json_url,
    )

    logger.info(
        AWSLogEvents.ScanConfigured,
        config=str(config),
        reader=str(artifact_reader.__class__),
        writer=str(artifact_writer.__class__),
    )
    print("Beginning AWS Account Scan")

    _, graph_set = run_scan(
        muxer=muxer,
        config=config,
        aws_resource_region_mapping_repo=aws_resource_region_mapping_repo,
        artifact_writer=artifact_writer,
        artifact_reader=artifact_reader,
    )
    print("AWS Account Scan Complete. Beginning write to Amazon Neptune.")
    logger.info(LogEvent.NeptuneGremlinWriteStart)
    graph = graph_set.to_neptune_lpg(scan_id)
    if config.neptune is None:
        raise Exception("Can not load to Neptune because config.neptune is empty.")
    endpoint = NeptuneEndpoint(
        host=config.neptune.host,
        port=config.neptune.port,
        region=config.neptune.region,
        ssl=bool(config.neptune.ssl),
        auth_mode=str(config.neptune.auth_mode),
    )
    neptune_client = AltimeterNeptuneClient(max_age_min=1440, neptune_endpoint=endpoint)
    neptune_client.write_to_neptune_lpg(graph, scan_id)
    logger.info(LogEvent.NeptuneGremlinWriteEnd)
    print("Write to Amazon Neptune Complete")


def aws2neptune_rdf(scan_id: str, config: AWSConfig, muxer: AWSScanMuxer) -> None:
    """Scan AWS resources to json, convert to RDF and load into Neptune
    if config.neptune is defined"""

    artifact_reader = ArtifactReader.from_artifact_path(config.artifact_path)
    artifact_writer = ArtifactWriter.from_artifact_path(
        artifact_path=config.artifact_path, scan_id=scan_id
    )

    aws_resource_region_mapping_repo = build_aws_resource_region_mapping_repo(
        global_region_whitelist=config.scan.regions,
        preferred_account_scan_regions=config.scan.preferred_account_scan_regions,
        services_regions_json_url=config.services_regions_json_url,
    )

    logger.info(
        AWSLogEvents.ScanConfigured,
        config=str(config),
        reader=str(artifact_reader.__class__),
        writer=str(artifact_writer.__class__),
    )
    print("Beginning AWS Account Scan")
    _, graph_set = run_scan(
        muxer=muxer,
        config=config,
        aws_resource_region_mapping_repo=aws_resource_region_mapping_repo,
        artifact_writer=artifact_writer,
        artifact_reader=artifact_reader,
    )
    print("AWS Account Scan Complete. Beginning write to Amazon Neptune.")
    logger.info(LogEvent.NeptuneRDFWriteStart)
    graph = graph_set.to_rdf()
    if config.neptune is None:
        raise Exception("Can not load to Neptune because config.neptune is empty.")
    endpoint = NeptuneEndpoint(
        host=config.neptune.host,
        port=config.neptune.port,
        region=config.neptune.region,
        ssl=bool(config.neptune.ssl),
        auth_mode=str(config.neptune.auth_mode),
    )
    neptune_client = AltimeterNeptuneClient(max_age_min=1440, neptune_endpoint=endpoint)
    neptune_client.write_to_neptune_rdf(graph)
    logger.info(LogEvent.NeptuneRDFWriteEnd)
    print("Write to Amazon Neptune Complete")


def generate_scan_id() -> str:
    """Generate a unique scan id"""
    now = datetime.now()
    scan_date = now.strftime("%Y%m%d")
    scan_time = str(int(now.timestamp()))
    scan_id = "/".join((scan_date, scan_time, str(uuid.uuid4())))
    return scan_id


def main(argv: Optional[List[str]] = None) -> int:
    """Main method for running a AWS to Neptune run"""
    if argv is None:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument("model", type=str, nargs="?")
    parser.add_argument("config", type=str, nargs="?")
    args_ns = parser.parse_args(argv)

    if args_ns.model is None:
        print("You need to specify the data model desired for this run (RDF or LPG).")
        sys.exit()
    if args_ns.model.lower() != "rdf" and args_ns.model.lower() != "lpg":
        print("You need to specify the data model desired for this run (RDF or LPG).")
        sys.exit()

    config_path = (
        "/home/ec2-user/graph_notebook_config.json" if args_ns.config is None else args_ns.config
    )

    with open(config_path) as f:
        config_dict = json.load(f)

    current_account = boto3.client("sts").get_caller_identity().get("Account")
    current_region = boto3.session.Session().region_name

    config = AWSConfig(
        artifact_path="./altimeter_runs",
        pruner_max_age_min=4320,
        graph_name="alti",
        concurrency=ConcurrencyConfig(max_account_scan_threads=1, max_svc_scan_threads=64,),
        scan=ScanConfig(
            accounts=[current_account],
            regions=[current_region],
            scan_sub_accounts=False,
            preferred_account_scan_regions=[current_region],
        ),
        neptune=NeptuneConfig(
            host=config_dict["host"],
            port=int(config_dict["port"]),
            auth_mode=config_dict["auth_mode"],
            iam_credentials_provider_type=config_dict["iam_credentials_provider_type"],
            ssl=config_dict["ssl"],
            region=config_dict["aws_region"],
            use_lpg=bool(args_ns.model.lower() == "lpg"),
        ),
    )

    if config.scan.ignored_resources:
        resource_spec_classes_list: List[Type[AWSResourceSpec]] = []
        for resource_spec_class in DEFAULT_RESOURCE_SPEC_CLASSES:
            if resource_spec_class.get_full_type_name() not in config.scan.ignored_resources:
                resource_spec_classes_list.append(resource_spec_class)
        resource_spec_classes = tuple(resource_spec_classes_list)
    else:
        resource_spec_classes = DEFAULT_RESOURCE_SPEC_CLASSES

    scan_id = generate_scan_id()
    muxer = LocalAWSScanMuxer(
        scan_id=scan_id, config=config, resource_spec_classes=resource_spec_classes
    )

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
