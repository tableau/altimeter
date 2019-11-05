#!/usr/bin/env python3
"""Convert intermediate JSON to RDF."""
import io
import gzip
import json
import sys
import urllib

import boto3

from altimeter.core.awslambda import get_required_lambda_env_var
from altimeter.core.graph.graph_set import GraphSet
from altimeter.core.log import Logger, LogEvent


def lambda_handler(event, context):
    json_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    json_key = urllib.parse.unquote(event["Records"][0]["s3"]["object"]["key"])
    rdf_bucket = get_required_lambda_env_var("RDF_BUCKET")
    rdf_key = ".".join(json_key.split(".")[:-1]) + ".rdf.gz"
    session = boto3.Session()
    s3_client = session.client("s3")

    logger = Logger()
    with logger.bind(json_bucket=json_bucket, json_key=json_key):
        logger.info(event=LogEvent.ReadFromS3Start)
        with io.BytesIO() as json_bytes_buf:
            s3_client.download_fileobj(json_bucket, json_key, json_bytes_buf)
            json_bytes_buf.flush()
            json_bytes_buf.seek(0)
            graph_set_bytes = json_bytes_buf.read()
            logger.info(event=LogEvent.ReadFromS3End)
        graph_set_str = graph_set_bytes.decode("utf-8")
        graph_set_dict = json.loads(graph_set_str)
        graph_set = GraphSet.from_dict(graph_set_dict)
        graph = graph_set.to_rdf()

    with logger.bind(rdf_bucket=rdf_bucket, rdf_key=rdf_key):
        logger.info(event=LogEvent.WriteToS3Start)
        with io.BytesIO() as rdf_bytes_buf:
            with gzip.GzipFile(fileobj=rdf_bytes_buf, mode="wb") as gz:
                graph.serialize(gz)
            rdf_bytes_buf.flush()
            rdf_bytes_buf.seek(0)
            s3_client.upload_fileobj(rdf_bytes_buf, rdf_bucket, rdf_key)
            s3_client.put_object_tagging(
                Bucket=rdf_bucket,
                Key=rdf_key,
                Tagging={
                    "TagSet": [
                        {"Key": "name", "Value": graph_set.name},
                        {"Key": "version", "Value": graph_set.version},
                        {"Key": "start_time", "Value": str(graph_set.start_time)},
                        {"Key": "end_time", "Value": str(graph_set.end_time)},
                    ]
                },
            )
        logger.info(event=LogEvent.WriteToS3End)


def main(argv=None):
    import argparse

    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("infile", type=str)
    parser.add_argument("outfile", type=str)
    args_ns = parser.parse_args(argv)
    with open(args_ns.infile, "r") as in_fp:
        graph_set_dict = json.load(in_fp)
    graph_set = GraphSet.from_dict(data=graph_set_dict)
    graph = graph_set.to_rdf()
    graph.serialize(args_ns.outfile)


if __name__ == "__main__":
    sys.exit(main())
