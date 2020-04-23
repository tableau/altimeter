from typing import Tuple, Set, List, Dict

from altimeter.aws.auth.accessor import Accessor
from altimeter.aws.log_events import AWSLogEvents
from altimeter.aws.scan.account_scan_plan import AccountScanPlan
from altimeter.aws.scan.muxer import AWSScanMuxer
from altimeter.aws.scan.scan_manifest import ScanManifest
from altimeter.core.artifact_io.reader import ArtifactReader
from altimeter.core.artifact_io.writer import ArtifactWriter
from altimeter.core.config import Config
from altimeter.core.graph.graph_set import GraphSet
from altimeter.core.log import Logger
from altimeter.core.multilevel_counter import MultilevelCounter


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
) -> Tuple[ScanManifest, GraphSet]:
    if config.scan.scan_sub_accounts:
        account_ids = get_sub_account_ids(config.scan.accounts, config.access.accessor)
    else:
        account_ids = config.scan.accounts
    account_scan_plan = AccountScanPlan(
        account_ids=account_ids, regions=config.scan.regions, accessor=config.access.accessor
    )
    logger = Logger()
    logger.info(event=AWSLogEvents.ScanAWSAccountsStart)
    # now combine account_scan_results and org_details to build a ScanManifest
    scanned_accounts: List[str] = []
    artifacts: List[str] = []
    errors: Dict[str, List[str]] = {}
    unscanned_accounts: List[str] = []
    stats = MultilevelCounter()
    graph_set = None

    for account_scan_manifest in muxer.scan(account_scan_plan=account_scan_plan):
        account_id = account_scan_manifest.account_id
        if account_scan_manifest.artifacts:
            for account_scan_artifact in account_scan_manifest.artifacts:
                artifacts.append(account_scan_artifact)
                artifact_graph_set_dict = artifact_reader.read_json(account_scan_artifact)
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
    if graph_set is None:
        raise Exception("BUG: No graph_set generated.")
    master_artifact_path = artifact_writer.write_json(name="master", data=graph_set.to_dict())
    logger.info(event=AWSLogEvents.ScanAWSAccountsEnd)
    start_time = graph_set.start_time
    end_time = graph_set.end_time
    scan_manifest = ScanManifest(
        scanned_accounts=scanned_accounts,
        master_artifact=master_artifact_path,
        artifacts=artifacts,
        errors=errors,
        unscanned_accounts=unscanned_accounts,
        api_call_stats=stats.to_dict(),
        start_time=start_time,
        end_time=end_time,
    )
    artifact_writer.write_json("manifest", data=scan_manifest.to_dict())
    return scan_manifest, graph_set
