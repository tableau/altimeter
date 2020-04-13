#!/usr/bin/env python3
"""Pull data from AWS and convert it to JSON for consumption by json2rdf.py"""
from datetime import datetime
import argparse
from pathlib import Path
import sys
from typing import Dict, List, Set

from altimeter.aws.auth.accessor import Accessor
from altimeter.core.awslambda import get_required_lambda_env_var
from altimeter.core.config import Config
from altimeter.core.graph.graph_set import GraphSet
from altimeter.core.log import Logger
from altimeter.aws.log import AWSLogEvents
from altimeter.core.multilevel_counter import MultilevelCounter

from altimeter.core.artifact_io.reader import ArtifactReader, FileArtifactReader, S3ArtifactReader
from altimeter.core.artifact_io.writer import ArtifactWriter, FileArtifactWriter, S3ArtifactWriter

from altimeter.aws.scan.muxer import AWSScanMuxer
from altimeter.aws.scan.muxer.local_muxer import LocalAWSScanMuxer
from altimeter.aws.scan.muxer.lambda_muxer import LambdaAWSScanMuxer
from altimeter.aws.scan.account_scan_plan import AccountScanPlan
from altimeter.aws.scan.scan_manifest import ScanManifest


def lambda_handler(event, context):
    account_scan_lambda_name = get_required_lambda_env_var("ACCOUNT_SCAN_LAMBDA_NAME")
    account_scan_lambda_timeout_str = get_required_lambda_env_var("ACCOUNT_SCAN_LAMBDA_TIMEOUT")
    try:
        account_scan_lambda_timeout = int(account_scan_lambda_timeout_str)
    except ValueError as ve:
        raise Exception(f'Parameter "ACCOUNT_SCAN_LAMBDA_TIMEOUT" must be an int: {ve}')
    json_bucket = get_required_lambda_env_var("JSON_BUCKET")

    config = Config.from_file(Path("./conf/lambda.toml"))

    logger = Logger()
    logger.info(
        AWSLogEvents.ScanConfigured,
        config=str(config),
        json_bucket=json_bucket,
        account_scan_lambda_name=account_scan_lambda_name,
        account_scan_lambda_timeout=account_scan_lambda_timeout,
    )

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
        config=config,
    )

    scan_manifest = scan(
        muxer=muxer,
        config=config,
        artifact_writer=artifact_writer,
        artifact_reader=artifact_reader,
    )

    artifact_writer.write_artifact("manifest", scan_manifest.to_dict())


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("output_dir", type=Path)
    args_ns = parser.parse_args(argv)

    config = Config.from_file(filepath=args_ns.config)

    now = datetime.now()
    scan_date = now.strftime("%Y%m%d")
    scan_time = str(int(now.timestamp()))

    output_dir = Path(args_ns.output_dir, scan_date, scan_time)

    logger = Logger()
    logger.info(
        AWSLogEvents.ScanConfigured,
        config=str(config),
        output_dir=output_dir,
    )

    muxer = LocalAWSScanMuxer(
        output_dir=output_dir,
        config=config,
    )

    artifact_writer = FileArtifactWriter(output_dir=output_dir)
    artifact_reader = FileArtifactReader()

    scan_manifest = scan(
        muxer=muxer,
        config=config,
        artifact_writer=artifact_writer,
        artifact_reader=artifact_reader,
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
    config: Config,
    artifact_writer: ArtifactWriter,
    artifact_reader: ArtifactReader,
) -> ScanManifest:
    if config.scan.scan_sub_accounts:
        account_ids = get_sub_account_ids(config.scan.accounts, config.access.accessor)
    else:
        account_ids = config.scan.accounts
    account_scan_plan = AccountScanPlan(account_ids=account_ids,
                                        regions=config.scan.regions,
                                        accessor=config.access.accessor)
    logger = Logger()
    logger.info(event=AWSLogEvents.ScanAWSAccountsStart)
    # now combine account_scan_results and org_details to build a ScanManifest
    scanned_accounts: List[Dict[str, str]] = []
    artifacts: List[str] = []
    errors: Dict[str, List[str]] = {}
    unscanned_accounts: List[Dict[str, str]] = []
    stats = MultilevelCounter()
    graph_set = None

    for account_scan_manifest in muxer.scan(account_scan_plan=account_scan_plan):
        account_id = account_scan_manifest.account_id
        if account_scan_manifest.artifacts:
            for account_scan_artifact in account_scan_manifest.artifacts:
                artifacts += account_scan_artifact
                artifact_graph_set_dict = artifact_reader.read_artifact(account_scan_artifact)
                artifact_graph_set = GraphSet.from_dict(artifact_graph_set_dict)
                if graph_set is None:
                    graph_set = artifact_graph_set
                else:
                    graph_set.merge(artifact_graph_set)
            if account_scan_manifest.errors:
                errors[account_id] = account_scan_manifest.errors
                unscanned_accounts.append(account_id)
            else:
                scanned_accounts.append(account_id)
        else:
            unscanned_accounts.append(account_id)
        account_stats = MultilevelCounter.from_dict(account_scan_manifest.api_call_stats)
        stats.merge(account_stats)
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
