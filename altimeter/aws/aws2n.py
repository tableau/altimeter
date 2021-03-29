import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import boto3
from pydantic import BaseSettings
from pydantic.json import pydantic_encoder

from altimeter.aws.log_events import AWSLogEvents
from altimeter.aws.resource_service_region_mapping import build_aws_resource_region_mapping_repo
from altimeter.aws.scan.muxer import AWSScanMuxer
from altimeter.aws.scan.scan import run_scan
from altimeter.core.artifact_io import parse_s3_uri
from altimeter.core.artifact_io.reader import ArtifactReader
from altimeter.core.artifact_io.writer import ArtifactWriter, GZIP
from altimeter.core.config import AWSConfig
from altimeter.core.log import Logger
from altimeter.core.log_events import LogEvent

from altimeter.core.neptune.client import GraphMetadata, NeptuneEndpoint, AltimeterNeptuneClient


class AWS2NConfig(BaseSettings):
    config_path: str
    account_scan_lambda_name: str
    account_scan_lambda_timeout: int


@dataclass(frozen=True)
class AWS2NResult:
    rdf_path: str
    graph_metadata: Optional[GraphMetadata]
    json_path: Optional[str] = None


def generate_scan_id() -> str:
    """Generate a unique scan id"""
    now = datetime.now()
    scan_date = now.strftime("%Y%m%d")
    scan_time = str(int(now.timestamp()))
    scan_id = "/".join((scan_date, scan_time, str(uuid.uuid4())))
    return scan_id


def aws2n(scan_id: str, config: AWSConfig, muxer: AWSScanMuxer, load_neptune: bool) -> AWS2NResult:
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
        aws_resource_region_mapping_repo=aws_resource_region_mapping_repo,
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
            bucket=rdf_bucket, key=rdf_key, load_iam_role_arn=str(config.neptune.iam_role_arn)
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
    return AWS2NResult(rdf_path=rdf_path, graph_metadata=graph_metadata, json_path=json_path)
