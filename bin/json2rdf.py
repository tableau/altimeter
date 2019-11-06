#!/usr/bin/env python3
"""Convert intermediate JSON to RDF."""
from dataclasses import dataclass
import io
import gzip
import json
import sys
import urllib

import boto3
from botocore.client import BaseClient
from rdflib import Graph

from altimeter.core.awslambda import get_required_lambda_env_var
from altimeter.core.graph.graph_set import GraphSet
from altimeter.core.log import Logger, LogEvent


@dataclass(frozen=True)
class GraphPackage:
    """Represents a rdflib.Graph and associated metadata."""

    graph: Graph
    name: str
    version: str
    start_time: int
    end_time: int


def graph_set_from_s3(s3_client: BaseClient, json_bucket: str, json_key: str) -> GraphSet:
    """Load a GraphSet from json located in an s3 object."""
    logger = Logger()
    logger.info(event=LogEvent.ReadFromS3Start)
    with io.BytesIO() as json_bytes_buf:
        s3_client.download_fileobj(json_bucket, json_key, json_bytes_buf)
        json_bytes_buf.flush()
        json_bytes_buf.seek(0)
        graph_set_bytes = json_bytes_buf.read()
        logger.info(event=LogEvent.ReadFromS3End)
    graph_set_str = graph_set_bytes.decode("utf-8")
    graph_set_dict = json.loads(graph_set_str)
    return GraphSet.from_dict(graph_set_dict)


def graph_pkg_from_s3(s3_client: BaseClient, json_bucket: str, json_key: str) -> GraphPackage:
    """Create an GraphPackage object from json content in an s3 object."""
    graph_set = graph_set_from_s3(s3_client=s3_client, json_bucket=json_bucket, json_key=json_key)
    return GraphPackage(
        graph=graph_set.to_rdf(),
        name=graph_set.name,
        version=graph_set.version,
        start_time=graph_set.start_time,
        end_time=graph_set.end_time,
    )


def lambda_handler(event, context):
    json_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    json_key = urllib.parse.unquote(event["Records"][0]["s3"]["object"]["key"])
    rdf_bucket = get_required_lambda_env_var("RDF_BUCKET")
    rdf_key = ".".join(json_key.split(".")[:-1]) + ".rdf.gz"
    session = boto3.Session()
    s3_client = session.client("s3")

    logger = Logger()
    with logger.bind(json_bucket=json_bucket, json_key=json_key):
        graph_pkg = graph_pkg_from_s3(
            s3_client=s3_client, json_bucket=json_bucket, json_key=json_key
        )

    with logger.bind(rdf_bucket=rdf_bucket, rdf_key=rdf_key):
        logger.info(event=LogEvent.WriteToS3Start)
        with io.BytesIO() as rdf_bytes_buf:
            with gzip.GzipFile(fileobj=rdf_bytes_buf, mode="wb") as gz:
                graph_pkg.graph.serialize(gz)
            rdf_bytes_buf.flush()
            rdf_bytes_buf.seek(0)
            s3_client.upload_fileobj(rdf_bytes_buf, rdf_bucket, rdf_key)
            s3_client.put_object_tagging(
                Bucket=rdf_bucket,
                Key=rdf_key,
                Tagging={
                    "TagSet": [
                        {"Key": "name", "Value": graph_pkg.name},
                        {"Key": "version", "Value": graph_pkg.version},
                        {"Key": "start_time", "Value": str(graph_pkg.start_time)},
                        {"Key": "end_time", "Value": str(graph_pkg.end_time)},
                    ]
                },
            )
        logger.info(event=LogEvent.WriteToS3End)


def graph_set_from_filepath(filepath: str) -> GraphSet:
    with open(filepath, "r") as fp:
        graph_set_dict = json.load(fp)
    return GraphSet.from_dict(data=graph_set_dict)


def graph_from_filepath(filepath: str) -> Graph:
    graph_set = graph_set_from_filepath(filepath)
    return graph_set.to_rdf()


def main(argv=None):
    import argparse

    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("infile", type=str)
    parser.add_argument("outfile", type=str)
    args_ns = parser.parse_args(argv)
    graph = graph_from_filepath(args_ns.infile)
    graph.serialize(args_ns.outfile)


if __name__ == "__main__":
    sys.exit(main())
