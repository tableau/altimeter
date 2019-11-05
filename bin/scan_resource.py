#!/usr/bin/env python3
"""Tool to run a single Resource class scan. Useful during developing Resource classes
and their Schemas. Run without usage for details."""
import json
import sys
from typing import Type

import boto3

from altimeter.aws.resource.resource_spec import AWSResourceSpec
from altimeter.core.json_encoder import json_encoder
from altimeter.aws.scan.aws_accessor import AWSAccessor
from altimeter.aws.scan.settings import RESOURCE_SPEC_CLASSES


def main(argv=None):
    import argparse

    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "resource_spec_class",
        type=str,
        help="Name of class in altimeter.aws.scan.settings.RESOURCE_SPEC_CLASSES to scan",
    )
    parser.add_argument("region", type=str, help="AWS region name to scan")

    args_ns = parser.parse_args(argv)
    resource_spec_class_name = args_ns.resource_spec_class
    region = args_ns.region

    resource_spec_class: Type[AWSResourceSpec] = None
    for cls in RESOURCE_SPEC_CLASSES:
        if cls.__name__ == resource_spec_class_name:
            resource_spec_class = cls
    if resource_spec_class is None:
        print(
            (
                f"Unable to find a class named {resource_spec_class_name} in "
                f"altimeter.aws.scan.settings.RESOURCE_SPEC_CLASSES: {RESOURCE_SPEC_CLASSES}."
            )
        )
        sys.exit(1)

    session = boto3.Session(region_name=region)
    sts_client = session.client("sts")
    account_id = sts_client.get_caller_identity()["Account"]
    aws_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region)
    resource_scan_result = resource_spec_class.scan(aws_accessor)
    resource_scan_result_dict = resource_scan_result.to_dict()
    resource_scan_result_json = json.dumps(
        resource_scan_result_dict, indent=2, default=json_encoder
    )
    print(resource_scan_result_json)


if __name__ == "__main__":
    sys.exit(main())
