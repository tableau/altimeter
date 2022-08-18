#!/usr/bin/env python3
"""Run a query against the latest graph for a given name and optional version.
This finds the Neptune instance details based on naming conventions - instance identifier
should begin with 'alti-'. Discover behavior can be overridden by specifying the endpoint
information in argument --neptune_endpoint
"""
import argparse
import sys
from typing import List, Optional

from altimeter.core.neptune.client import (
    AltimeterNeptuneClient,
    discover_neptune_endpoint,
    NeptuneEndpoint,
)


def main(argv: Optional[List[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("query_file", type=str)
    parser.add_argument("--graph_names", type=str, default=["alti"], nargs="+")
    parser.add_argument("--historic_graph_names", type=str, nargs="+")
    parser.add_argument("--max_age_min", type=int, default=1440)
    parser.add_argument("--raw", default=False, action="store_true")
    parser.add_argument("--neptune_endpoint", help="Neptune endpoint specified as host:port:region")
    args_ns = parser.parse_args(argv)

    with open(args_ns.query_file, "r") as query_fp:
        query = query_fp.read()

    if args_ns.neptune_endpoint is not None:
        try:
            host, port_str, region = args_ns.neptune_endpoint.split(":")
            port: int = int(port_str)
        except ValueError:
            print(f"neptune_endpoint should be a string formatted as host:port:region")
            return 1
        endpoint = NeptuneEndpoint(host=host, port=port, region=region)
    else:
        endpoint = discover_neptune_endpoint()
    client = AltimeterNeptuneClient(max_age_min=args_ns.max_age_min, neptune_endpoint=endpoint)

    if args_ns.historic_graph_names:
        results = client.run_historic_query(graph_names=args_ns.historic_graph_names, query=query)
        print(results.to_csv(), end="")
    elif args_ns.raw:
        raw_results = client.run_raw_query(query=query)
        print(raw_results.to_csv(), end="")
    else:
        results = client.run_query(graph_names=args_ns.graph_names, query=query)
        print(results.to_csv(), end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
