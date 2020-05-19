#!/usr/bin/env python3
"""Run a raw query against neptune.
Run as a lambda this calls Neptune.
Run from the command line this finds the runrawquery lambda, sends a query to it and
reads the results from S3.
"""
import argparse
import hashlib
import json
import logging
import sys
import time
from typing import Any, Dict, List, Optional

import boto3

from altimeter.core.parameters import (
    get_required_str_env_var,
    get_required_int_env_var,
    get_required_lambda_event_var,
)
from altimeter.core.neptune.client import AltimeterNeptuneClient, NeptuneEndpoint


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)

    query = get_required_lambda_event_var(event, "query")
    if not isinstance(query, str):
        raise ValueError(f"Value for query should be a str. Is {type(query)}")

    host = get_required_str_env_var("NEPTUNE_HOST")
    port = get_required_int_env_var("NEPTUNE_PORT")
    region = get_required_str_env_var("NEPTUNE_REGION")
    results_bucket = get_required_str_env_var("RESULTS_BUCKET")

    endpoint = NeptuneEndpoint(host=host, port=port, region=region)
    client = AltimeterNeptuneClient(max_age_min=0, neptune_endpoint=endpoint)
    query_result = client.run_raw_query(query=query)

    csv_results = query_result.to_csv()

    query_hash = hashlib.sha256(query.encode()).hexdigest()
    now_str = str(int(time.time()))
    results_key = "/".join(("raw", query_hash, f"{now_str}.csv"))
    s3_client = boto3.Session().client("s3")
    s3_client.put_object(Bucket=results_bucket, Key=results_key, Body=csv_results)

    return {
        "results_bucket": results_bucket,
        "results_key": results_key,
        "num_results": query_result.length,
    }


def get_runrawquery_lambda_name() -> str:
    runquery_lambda_name_prefix = "ITCloudGraph-RunRawQuery-"
    lambda_client = boto3.client("lambda")
    paginator = lambda_client.get_paginator("list_functions")
    for resp in paginator.paginate():
        for func in resp["Functions"]:
            if func["FunctionName"].startswith(runquery_lambda_name_prefix):
                return func["FunctionName"]
    raise ValueError(
        (
            f"Unable to find a runquery lambda with name starting with "
            f"{runquery_lambda_name_prefix}"
        )
    )


def main(argv: Optional[List[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("query_file", type=str)
    args_ns = parser.parse_args(argv)

    with open(args_ns.query_file, "r") as query_fp:
        query = query_fp.read()

    runquery_lambda_name = get_runrawquery_lambda_name()

    payload = {
        "query": query,
    }
    payload_bytes = json.dumps(payload).encode("utf-8")
    lambda_client = boto3.client("lambda")
    invoke_lambda_resp = lambda_client.invoke(
        FunctionName=runquery_lambda_name, Payload=payload_bytes
    )
    lambda_resp_bytes = invoke_lambda_resp["Payload"].read()
    lambda_resp_str = lambda_resp_bytes.decode("utf-8")
    lambda_resp = json.loads(lambda_resp_str)
    if "errorMessage" in lambda_resp:
        print("Error running query:")
        print(lambda_resp["errorMessage"])
        sys.exit(1)
    results_bucket = lambda_resp["results_bucket"]
    results_key = lambda_resp["results_key"]
    s3_client = boto3.client("s3")
    s3_resp = s3_client.get_object(Bucket=results_bucket, Key=results_key)
    results_bytes = s3_resp["Body"].read()
    results_str = results_bytes.decode("utf-8")
    print(results_str)
    return 0


if __name__ == "__main__":
    sys.exit(main())
