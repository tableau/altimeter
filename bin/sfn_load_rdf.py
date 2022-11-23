#!/usr/bin/env python3
"""LoadRDF StepFunction Lambda
Load an rdf graph into Neptune"""
import json
import logging
from typing import Any, Dict

import boto3

from altimeter.core.artifact_io import parse_s3_uri
from altimeter.core.base_model import BaseImmutableModel
from altimeter.core.config import AWSConfig
from altimeter.core.log import Logger
from altimeter.core.log_events import LogEvent
from altimeter.core.neptune.client import GraphMetadata, NeptuneEndpoint, AltimeterNeptuneClient


class LoadRDFInput(BaseImmutableModel):
    config: AWSConfig
    rdf_path: str


class LoadRDFOutput(BaseImmutableModel):
    graph_metadata: GraphMetadata


def lambda_handler(event: Dict[str, Any], _: Any) -> Dict[str, Any]:
    """Lambda entrypoint"""
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    logger = Logger()
    load_rdf_input = LoadRDFInput(**event)

    if load_rdf_input.config.neptune is None:
        raise ValueError("Input config must include neptune section")

    endpoint = NeptuneEndpoint(
        host=load_rdf_input.config.neptune.host,
        port=load_rdf_input.config.neptune.port,
        region=load_rdf_input.config.neptune.region,
    )
    neptune_client = AltimeterNeptuneClient(max_age_min=1440, neptune_endpoint=endpoint)
    rdf_bucket, rdf_key = parse_s3_uri(load_rdf_input.rdf_path)
    if rdf_key is None:
        raise Exception(f"Invalid rdf s3 path {load_rdf_input.rdf_path}")
    graph_metadata = neptune_client.load_graph(
        bucket=rdf_bucket,
        key=rdf_key,
        load_iam_role_arn=str(load_rdf_input.config.neptune.iam_role_arn),
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
        TopicArn=load_rdf_input.config.neptune.graph_load_sns_topic_arn,
        MessageStructure="json",
        Message=json.dumps(message_dict),
    )
    logger.info(event=LogEvent.GraphLoadedSNSNotificationEnd)
    return LoadRDFOutput(graph_metadata=graph_metadata).dict()
