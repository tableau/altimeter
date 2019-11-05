#!/usr/bin/env python3
"""Load RDF from S3 into Neptune"""
import json
import urllib

import boto3

from altimeter.core.awslambda import get_required_lambda_env_var
from altimeter.core.log import Logger, LogEvent
from altimeter.core.neptune.client import AltimeterNeptuneClient, NeptuneEndpoint


def lambda_handler(event, context):
    rdf_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    rdf_key = urllib.parse.unquote(event["Records"][0]["s3"]["object"]["key"])

    neptune_host = get_required_lambda_env_var("NEPTUNE_HOST")
    neptune_port = get_required_lambda_env_var("NEPTUNE_PORT")
    neptune_region = get_required_lambda_env_var("NEPTUNE_REGION")
    neptune_load_iam_role_arn = get_required_lambda_env_var("NEPTUNE_LOAD_IAM_ROLE_ARN")
    on_success_sns_topic_arn = get_required_lambda_env_var("ON_SUCCESS_SNS_TOPIC_ARN")

    endpoint = NeptuneEndpoint(host=neptune_host, port=neptune_port, region=neptune_region)
    neptune_client = AltimeterNeptuneClient(max_age_min=1440, neptune_endpoint=endpoint)
    graph_metadata = neptune_client.load_graph(
        bucket=rdf_bucket, key=rdf_key, load_iam_role_arn=neptune_load_iam_role_arn
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
        TopicArn=on_success_sns_topic_arn, MessageStructure="json", Message=json.dumps(message_dict)
    )
    logger.info(event=LogEvent.GraphLoadedSNSNotificationEnd)
