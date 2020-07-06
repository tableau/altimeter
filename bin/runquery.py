#!/usr/bin/env python3
"""Run a query against the latest graph for a given name and optional version.
This finds the Neptune instance details based on naming conventions - instance identifier
should begin with 'alti-'
"""
import argparse
import sys
from typing import List, Optional

from altimeter.core.neptune.client import AltimeterNeptuneClient, discover_neptune_endpoint


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

    endpoint = discover_neptune_endpoint()
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
