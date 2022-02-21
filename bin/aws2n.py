#!/usr/bin/env python3
"""Graph AWS resource data in Neptune"""
import argparse
import os
import sys
from typing import List, Optional, Type

from altimeter.aws.aws2n import generate_scan_id, aws2n
from altimeter.aws.resource.resource_spec import AWSResourceSpec
from altimeter.aws.scan.muxer.local_muxer import LocalAWSScanMuxer
from altimeter.aws.scan.settings import DEFAULT_RESOURCE_SPEC_CLASSES
from altimeter.core.config import AWSConfig


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

    config = AWSConfig.from_path(config)
    if config.scan.ignored_resources:
        resource_spec_classes_list: List[Type[AWSResourceSpec]] = []
        for resource_spec_class in DEFAULT_RESOURCE_SPEC_CLASSES:
            if resource_spec_class.get_full_type_name() not in config.scan.ignored_resources:
                resource_spec_classes_list.append(resource_spec_class)
        resource_spec_classes = tuple(resource_spec_classes_list)
    else:
        resource_spec_classes = DEFAULT_RESOURCE_SPEC_CLASSES
    scan_id = generate_scan_id()
    muxer = LocalAWSScanMuxer(
        scan_id=scan_id, config=config, resource_spec_classes=resource_spec_classes,
    )
    result = aws2n(scan_id=scan_id, config=config, muxer=muxer, load_neptune=False)
    print(result.rdf_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
