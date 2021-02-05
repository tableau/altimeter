#!/usr/bin/env python3
"""Graph AWS resource data in Neptune"""
import argparse
import logging
import os
import sys
from typing import Any, Dict, List, Optional

from altimeter.aws.aws2n import AWS2NConfig, generate_scan_id, aws2n
from altimeter.aws.scan.muxer.lambda_muxer import LambdaAWSScanMuxer
from altimeter.aws.scan.muxer.local_muxer import LocalAWSScanMuxer
from altimeter.core.config import Config


def lambda_handler(_: Dict[str, Any], __: Any) -> None:
    """AWS Lambda Handler"""
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)

    aws2n_config = AWS2NConfig()

    config = Config.from_path(path=aws2n_config.config_path)

    scan_id = generate_scan_id()
    muxer = LambdaAWSScanMuxer(
        scan_id=scan_id,
        config=config,
        account_scan_lambda_name=aws2n_config.account_scan_lambda_name,
        account_scan_lambda_timeout=aws2n_config.account_scan_lambda_timeout,
    )
    aws2n(scan_id=scan_id, config=config, muxer=muxer, load_neptune=True)


def main(argv: Optional[List[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=str, nargs="?")
    args_ns = parser.parse_args(argv)

    config = args_ns.config
    if config is None:
        config = os.environ.get("CONFIG_PATH")
    if config is None:
        print("config must be provided as a positional arg or env var 'CONFIG_PATH'")
        return 1

    config = Config.from_path(config)
    scan_id = generate_scan_id()
    muxer = LocalAWSScanMuxer(scan_id=scan_id, config=config)
    result = aws2n(scan_id=scan_id, config=config, muxer=muxer, load_neptune=False)
    print(result.rdf_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
