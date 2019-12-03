#!/usr/bin/env python3
"""Pull data from AWS and convert it to JSON for consumption by json2rdf.py.
Run with -h for documentation."""
LOCAL_USAGE = """

This tool can be run locally in two modes:

Single Account Scan Mode
========================

The currently configured AWS credentials will be used to scan a single account.
Assuming your local environment has AWS credentials configured (via ~/.aws/credentials
and/or various AWS_* env vars) the following will run a scan of the current account in
all regions and save results under /tmp:

    bin/aws2json.py --base_dir=/tmp

Specific regions can be specified using the --regions arg:

    bin/aws2json.py --base_dir /tmp --regions us-east-1 us-west-2

Multiple Account Scan Mode
==========================

To scan multiple accounts two additional parameters must be specified:

    --accounts - a list of accounts to scan
    --access_config - filepath of an access config json file

For example:

    bin/aws2json.py --accounts 012345678901 234567789012 --access_config ~/access_config.json --base_dir ~/test

The access_config.json file specifies a set of steps which can be used to access an account
via STS assume role operations. See the Altimeter user documentation for more information.
"""
from datetime import datetime
import argparse
import os
from pathlib import Path
import sys
from typing import Dict, List, Set

from altimeter.aws.access.accessor import Accessor
from altimeter.core.awslambda import get_required_lambda_env_var
from altimeter.core.graph.graph_set import GraphSet
from altimeter.core.log import Logger
from altimeter.aws.log import AWSLogEvents
from altimeter.core.multilevel_counter import MultilevelCounter

from altimeter.core.artifact_io.reader import ArtifactReader, FileArtifactReader, S3ArtifactReader
from altimeter.core.artifact_io.writer import ArtifactWriter, FileArtifactWriter, S3ArtifactWriter

from altimeter.aws.scan.muxer import AWSScanMuxer
from altimeter.aws.scan.muxer.local_muxer import LocalAWSScanMuxer
from altimeter.aws.scan.muxer.lambda_muxer import LambdaAWSScanMuxer
from altimeter.aws.scan.account_scan_plan import build_account_scan_plans
from altimeter.aws.scan.scan_manifest import ScanManifest

import boto3

DEFAULT_MAX_LAMBDAS = 192


def lambda_handler(event, context):
    account_scan_lambda_name = get_required_lambda_env_var("ACCOUNT_SCAN_LAMBDA_NAME")
    account_scan_lambda_timeout_str = get_required_lambda_env_var("ACCOUNT_SCAN_LAMBDA_TIMEOUT")
    try:
        account_scan_lambda_timeout = int(account_scan_lambda_timeout_str)
    except ValueError as ve:
        raise Exception(f'Parameter "ACCOUNT_SCAN_LAMBDA_TIMEOUT" must be an int: {ve}')
    json_bucket = get_required_lambda_env_var("JSON_BUCKET")
    if "accounts" in event:
        account_ids = event["accounts"]
    else:
        account_ids = get_required_lambda_env_var("ACCOUNTS").split(",")
    if "regions" in event:
        regions = event["regions"]
    else:
        regions = os.environ.get("regions", [])
    if "scan_sub_accounts" in event:
        scan_sub_accounts = bool(event["scan_sub_accounts"])
    else:
        scan_sub_accounts = bool(os.environ.get("SCAN_SUB_ACCOUNTS", True))
    max_lambdas = int(os.environ.get("MAX_LAMBDAS", DEFAULT_MAX_LAMBDAS))

    logger = Logger()
    logger.info(
        AWSLogEvents.ScanConfigured,
        account_ids=account_ids,
        regions=regions,
        scan_sub_accounts=scan_sub_accounts,
        json_bucket=json_bucket,
        account_scan_lambda_name=account_scan_lambda_name,
        account_scan_lambda_timeout=account_scan_lambda_timeout,
    )

    accessor = Accessor.from_file(Path("./config/access_config.json"))

    now = datetime.now()
    scan_date = now.strftime("%Y%m%d")
    scan_time = str(int(now.timestamp()))

    key_prefix = "/".join((scan_date, scan_time))
    artifact_writer = S3ArtifactWriter(bucket=json_bucket, key_prefix=key_prefix)
    artifact_reader = S3ArtifactReader()

    muxer = LambdaAWSScanMuxer(
        account_scan_lambda_name=account_scan_lambda_name,
        account_scan_lambda_timeout=account_scan_lambda_timeout,
        json_bucket=json_bucket,
        key_prefix=key_prefix,
        scan_sub_accounts=scan_sub_accounts,
        max_lambdas=max_lambdas,
    )

    scan_manifest = scan(
        muxer=muxer,
        account_ids=account_ids,
        regions=regions,
        accessor=accessor,
        artifact_writer=artifact_writer,
        artifact_reader=artifact_reader,
        scan_sub_accounts=scan_sub_accounts,
    )

    artifact_writer.write_artifact("manifest", scan_manifest.to_dict())


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser(
        description=LOCAL_USAGE, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--access_config", required=False, type=Path)
    parser.add_argument("--accounts", required=False, type=str, nargs="*")
    parser.add_argument("--regions", required=False, type=str, nargs="*")
    parser.add_argument("--base_dir", required=True, type=str)
    parser.add_argument("--scan_sub_accounts", required=False, action="store_true")
    args_ns = parser.parse_args(argv)

    regions = args_ns.regions if args_ns.regions else []
    base_dir = args_ns.base_dir
    scan_sub_accounts = args_ns.scan_sub_accounts

    logger = Logger()

    # there are two run modes - if access_config and accounts are specified,
    # it is a multi-account scan.  Otherwise it is assumed that the creds which are
    # currently set in env vars will be the target scan account for a single-account scan.
    if any((args_ns.access_config, args_ns.accounts)):
        # multi-account run mode
        if all((args_ns.access_config, args_ns.accounts)):
            accessor = Accessor.from_file(args_ns.access_config)
            account_ids = args_ns.accounts
            logger.info(
                AWSLogEvents.ScanConfigured,
                account_ids=account_ids,
                regions=regions,
                scan_sub_accounts=scan_sub_accounts,
                base_dir=base_dir,
                access_config_path=args_ns.access_config,
            )
        else:
            parser.error("Must either specify both access_config and account_ids or neither.")
    else:
        # single-account run mode
        if scan_sub_accounts:
            raise ValueError("scan_sub_accounts not supported in single-account scan mode.")
        sts_client = boto3.client("sts")
        account_id = sts_client.get_caller_identity()["Account"]
        account_ids = [account_id]
        accessor = Accessor()
        logger.info(
            AWSLogEvents.ScanConfigured, account_ids=account_ids, regions=regions, base_dir=base_dir
        )

    now = datetime.now()
    scan_date = now.strftime("%Y%m%d")
    scan_time = str(int(now.timestamp()))

    full_dir = Path(base_dir, scan_date, scan_time)

    muxer = LocalAWSScanMuxer(output_dir=full_dir, scan_sub_accounts=scan_sub_accounts)

    artifact_writer = FileArtifactWriter(output_dir=full_dir)
    artifact_reader = FileArtifactReader()

    scan_manifest = scan(
        muxer=muxer,
        account_ids=account_ids,
        regions=regions,
        accessor=accessor,
        artifact_writer=artifact_writer,
        artifact_reader=artifact_reader,
        scan_sub_accounts=scan_sub_accounts,
    )

    artifact_writer.write_artifact("manifest", scan_manifest.to_dict())
    print(scan_manifest.master_artifact)


def get_sub_account_ids(account_ids: List[str], accessor: Accessor) -> List[str]:
    logger = Logger()
    logger.info(event=AWSLogEvents.GetSubAccountsStart)
    sub_account_ids: Set[str] = set(account_ids)
    for master_account_id in account_ids:
        with logger.bind(master_account_id=master_account_id):
            account_session = accessor.get_session(master_account_id)
            orgs_client = account_session.client("organizations")
            resp = orgs_client.describe_organization()
            if resp["Organization"]["MasterAccountId"] == master_account_id:
                accounts_paginator = orgs_client.get_paginator("list_accounts")
                for accounts_resp in accounts_paginator.paginate():
                    for account_resp in accounts_resp["Accounts"]:
                        if account_resp["Status"].lower() == "active":
                            account_id = account_resp["Id"]
                            sub_account_ids.add(account_id)
    logger.info(event=AWSLogEvents.GetSubAccountsEnd)
    return list(sub_account_ids)


def scan(
    muxer: AWSScanMuxer,
    account_ids: List[str],
    regions: List[str],
    accessor: Accessor,
    artifact_writer: ArtifactWriter,
    artifact_reader: ArtifactReader,
    scan_sub_accounts: bool = False,
) -> ScanManifest:
    if scan_sub_accounts:
        account_ids = get_sub_account_ids(account_ids, accessor)
    account_scan_plans = build_account_scan_plans(
        accessor=accessor, account_ids=account_ids, regions=regions
    )
    logger = Logger()
    logger.info(event=AWSLogEvents.ScanAWSAccountsStart)
    account_scan_manifests = muxer.scan(account_scan_plans=account_scan_plans)
    # now combine account_scan_results and org_details to build a ScanManifest
    scanned_accounts: List[Dict[str, str]] = []
    artifacts: List[str] = []
    errors: Dict[str, List[str]] = {}
    unscanned_accounts: List[Dict[str, str]] = []
    stats = MultilevelCounter()

    for account_scan_manifest in account_scan_manifests:
        account_id = account_scan_manifest.account_id
        if account_scan_manifest.artifacts:
            artifacts += account_scan_manifest.artifacts
            if account_scan_manifest.errors:
                errors[account_id] = account_scan_manifest.errors
                unscanned_accounts.append(account_id)
            else:
                scanned_accounts.append(account_id)
        else:
            unscanned_accounts.append(account_id)
        account_stats = MultilevelCounter.from_dict(account_scan_manifest.api_call_stats)
        stats.merge(account_stats)
    graph_set = None
    for artifact_path in artifacts:
        artifact_dict = artifact_reader.read_artifact(artifact_path)
        artifact_graph_set = GraphSet.from_dict(artifact_dict)
        if graph_set is None:
            graph_set = artifact_graph_set
        else:
            graph_set.merge(artifact_graph_set)
    master_artifact_path = None
    if graph_set:
        master_artifact_path = artifact_writer.write_artifact(
            name="master", data=graph_set.to_dict()
        )
    logger.info(event=AWSLogEvents.ScanAWSAccountsEnd)
    if graph_set:
        start_time = graph_set.start_time
        end_time = graph_set.end_time
    else:
        start_time, end_time = None, None
    return ScanManifest(
        scanned_accounts=scanned_accounts,
        master_artifact=master_artifact_path,
        artifacts=artifacts,
        errors=errors,
        unscanned_accounts=unscanned_accounts,
        api_call_stats=stats.to_dict(),
        start_time=start_time,
        end_time=end_time,
    )


if __name__ == "__main__":
    sys.exit(main())
