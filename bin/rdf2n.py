#!/usr/bin/env python3
"""Load RDF from S3 into Neptune"""
import json
import logging
from typing import Any, Dict
import urllib.parse

import boto3

from altimeter.core.config import Config, InvalidConfigException
from altimeter.core.log import Logger
from altimeter.core.log_events import LogEvent
from altimeter.core.neptune.client import AltimeterNeptuneClient, NeptuneEndpoint
from altimeter.core.parameters import get_required_str_env_var


def lambda_handler(event: Dict[str, Any], context: Any) -> None:
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)

    rdf_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    rdf_key = urllib.parse.unquote(event["Records"][0]["s3"]["object"]["key"])

    config_path = get_required_str_env_var("CONFIG_PATH")
    config = Config.from_path(path=config_path)

    if config.neptune is None:
        raise InvalidConfigException("Configuration missing neptune section.")

    endpoint = NeptuneEndpoint(
        host=config.neptune.host, port=config.neptune.port, region=config.neptune.region
    )
    neptune_client = AltimeterNeptuneClient(max_age_min=1440, neptune_endpoint=endpoint)
    graph_metadata = neptune_client.load_graph(
        bucket=rdf_bucket, key=rdf_key, load_iam_role_arn=config.neptune.iam_role_arn,
    )

    logger = Logger()
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
