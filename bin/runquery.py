#!/usr/bin/env python3
"""Run a query against the latest graph for a given name and optional version.
This finds the Neptune instance details based on naming conventions - instance identifier
should begin with 'alti-'
"""
import argparse
import sys
from typing import List, Optional

import boto3

from altimeter.core.exceptions import AltimeterException
from altimeter.core.neptune.client import AltimeterNeptuneClient, NeptuneEndpoint


def get_neptune_endpoint() -> NeptuneEndpoint:
    """Find a Neptune"""
    instance_id_prefix = "alti-"
    neptune_client = boto3.client("neptune")
    paginator = neptune_client.get_paginator("describe_db_instances")
    for resp in paginator.paginate():
        for instance in resp.get("DBInstances", []):
            instance_id = instance.get("DBInstanceIdentifier")
            if instance_id:
                if instance_id.startswith(instance_id_prefix):
                    endpoint = instance["Endpoint"]
                    host = endpoint["Address"]
                    port = endpoint["Port"]
                    region = boto3.session.Session().region_name
                    return NeptuneEndpoint(host=host, port=port, region=region)
    raise AltimeterException(f"No Neptune instance found matching {instance_id_prefix}*")


def main(argv: Optional[List[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("query_file", type=str)
    parser.add_argument("--graph_names", type=str, default=["alti"], nargs="+")
    parser.add_argument("--max_age_min", type=int, default=1440)
    parser.add_argument("--raw", default=False, action="store_true")
    args_ns = parser.parse_args(argv)

    with open(args_ns.query_file, "r") as query_fp:
        query = query_fp.read()

    endpoint = get_neptune_endpoint()
    client = AltimeterNeptuneClient(max_age_min=args_ns.max_age_min, neptune_endpoint=endpoint)

    if args_ns.raw:
        raw_results = client.run_raw_query(query=query)
        print(raw_results.to_csv(), end="")
    else:
        results = client.run_query(graph_names=args_ns.graph_names, query=query)
        print(results.to_csv(), end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
