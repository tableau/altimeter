from typing import Dict, List, Optional, Set, Tuple

import boto3

from altimeter.aws.auth.accessor import Accessor
from altimeter.aws.log_events import AWSLogEvents
from altimeter.aws.scan.scan_plan import ScanPlan
from altimeter.aws.scan.muxer import AWSScanMuxer
from altimeter.aws.scan.scan_manifest import ScanManifest
from altimeter.core.artifact_io.reader import ArtifactReader
from altimeter.core.artifact_io.writer import ArtifactWriter
from altimeter.core.config import Config
from altimeter.core.graph.graph_set import GraphSet, ValidatedGraphSet
from altimeter.core.log import Logger


def get_sub_account_ids(account_ids: Tuple[str, ...], accessor: Accessor) -> Tuple[str, ...]:
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
    return tuple(sub_account_ids)


def run_scan(
    muxer: AWSScanMuxer,
    config: Config,
    artifact_writer: ArtifactWriter,
    artifact_reader: ArtifactReader,
) -> Tuple[ScanManifest, ValidatedGraphSet]:
    if config.scan.accounts:
        scan_account_ids = config.scan.accounts
    else:
        sts_client = boto3.client("sts")
        scan_account_id = sts_client.get_caller_identity()["Account"]
        scan_account_ids = (scan_account_id,)
    if config.scan.scan_sub_accounts:
        account_ids = get_sub_account_ids(scan_account_ids, config.accessor)
    else:
        account_ids = scan_account_ids
    scan_plan = ScanPlan(
        account_ids=account_ids, regions=config.scan.regions, accessor=config.accessor
    )
    logger = Logger()
    logger.info(event=AWSLogEvents.ScanAWSAccountsStart)
    # now combine account_scan_results and org_details to build a ScanManifest
    scanned_accounts: List[str] = []
    artifacts: List[str] = []
    errors: Dict[str, List[str]] = {}
    unscanned_accounts: Set[str] = set()
    graph_sets: List[GraphSet] = []

    for account_scan_manifest in muxer.scan(scan_plan=scan_plan):
        account_id = account_scan_manifest.account_id
        if account_scan_manifest.errors:
            errors[account_id] = account_scan_manifest.errors
            unscanned_accounts.add(account_id)
        if account_scan_manifest.artifacts:
            for account_scan_artifact in account_scan_manifest.artifacts:
                artifacts.append(account_scan_artifact)
                artifact_graph_set_dict = artifact_reader.read_json(account_scan_artifact)
                graph_sets.append(GraphSet.parse_obj(artifact_graph_set_dict))
            scanned_accounts.append(account_id)
        else:
            unscanned_accounts.add(account_id)
    if not graph_sets:
        raise Exception("BUG: No graph_sets generated.")
    validated_graph_set = ValidatedGraphSet.from_graph_set(GraphSet.from_graph_sets(graph_sets))
    master_artifact_path: Optional[str] = None
    if config.write_master_json:
        master_artifact_path = artifact_writer.write_json(name="master", data=validated_graph_set)
    logger.info(event=AWSLogEvents.ScanAWSAccountsEnd)
    start_time = validated_graph_set.start_time
    end_time = validated_graph_set.end_time
    scan_manifest = ScanManifest(
        scanned_accounts=scanned_accounts,
        master_artifact=master_artifact_path,
        artifacts=artifacts,
        errors=errors,
        unscanned_accounts=list(unscanned_accounts),
        start_time=start_time,
        end_time=end_time,
    )
    artifact_writer.write_json("manifest", data=scan_manifest)
    return scan_manifest, validated_graph_set
