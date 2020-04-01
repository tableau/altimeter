"""Base class for classes which scan AWS resources in a single account."""
import abc
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
import time
import traceback
from typing import Any, DefaultDict, Dict, List, Tuple, Type

import boto3

from altimeter.core.artifact_io.writer import ArtifactWriter
from altimeter.aws.auth.accessor import Accessor
from altimeter.aws.log import AWSLogEvents
from altimeter.aws.resource.resource_spec import ScanGranularity, AWSResourceSpec
from altimeter.aws.resource.unscanned_account import UnscannedAccountResourceSpec
from altimeter.aws.scan.account_scan_plan import AccountScanPlan
from altimeter.aws.scan.aws_accessor import AWSAccessor
from altimeter.core.graph.graph_set import GraphSet
from altimeter.core.graph.graph_spec import GraphSpec
from altimeter.core.log import Logger
from altimeter.core.multilevel_counter import MultilevelCounter


def get_all_enabled_regions(session: boto3.Session) -> Tuple[str, ...]:
    """Get all enabled regions -  which are either opted-in or are opt-in-not-required - for
    a given session.
    Args:
        session: boto3 Session

    Returns:
        tuple of enabled regions in the given session.
    """
    client = session.client("ec2")
    resp: Dict[str, List[Dict[str, str]]] = client.describe_regions(
        Filters=[{"Name": "opt-in-status", "Values": ["opt-in-not-required", "opted-in"]}]
    )
    regions = tuple(region["RegionName"] for region in resp["Regions"])
    return regions


class BaseScanner(abc.ABC):
    """Base class for classes which scan AWS resources in multiple or single accounts.

    Args:
        account_scan_plan: AccountScanPlan describing scan targets
        artifact_writer: ArtifactWriter for writing out artifacts
        graph_name: name of graph
        graph_version: version string for graph
        max_account_threads: max number of accounts to scan concurrently
        max_svc_threads: max number of service scan threads per account to run concurrently.
        resource_spec_classes: tuple of aws resource spec classes to scan.
    """

    def __init__(
        self,
        account_scan_plan: AccountScanPlan,
        artifact_writer: ArtifactWriter,
        max_account_threads: int,
        max_svc_threads: int,
        graph_name: str,
        graph_version: str,
        resource_spec_classes: Tuple[Type[AWSResourceSpec], ...],
    ) -> None:
        self.account_scan_plan = account_scan_plan
        self.artifact_writer = artifact_writer
        self.max_account_threads = max_account_threads
        self.max_svc_threads = max_svc_threads
        self.graph_name = graph_name
        self.graph_version = graph_version
        self.resource_spec_classes = resource_spec_classes

    def scan(self) -> List[Dict[str, Any]]:
        """Scan an account and return a list of dicts containing keys:

            * account_id: str
            * output_artifact: str
            * api_call_stats: Dict[str, Any]
            * errors: List[str]

        If errors is non-empty the results are incomplete for this account.
        output_artifact is a pointer to the actual scan data - either on the local fs or in s3.

        To scan an account we create a set of GraphSpecs, one for each region.  Any ACCOUNT
        level granularity resources are only scanned in a single region (e.g. IAM Users)

        Returns:
            List of Dict of scan result, one per account
        """
        logger = Logger()
        scan_results = []
        with logger.bind(
            account_ids=",".join(self.account_scan_plan.account_ids),
            regions=",".join(self.account_scan_plan.regions),
        ):
            with ThreadPoolExecutor(max_workers=self.max_account_threads) as executor:
                logger.info(event=AWSLogEvents.ScanAWSAccountBatchStart)
                futures = []
                for account_id in self.account_scan_plan.account_ids:
                    with logger.bind(account_id=account_id):
                        scan_future = schedule_scan_account(
                            executor=executor,
                            account_id=account_id,
                            regions=self.account_scan_plan.regions,
                            resource_spec_classes=self.resource_spec_classes,
                            graph_name=self.graph_name,
                            graph_version=self.graph_version,
                            max_svc_threads=self.max_svc_threads,
                            artifact_writer=self.artifact_writer,
                            accessor_dict=self.account_scan_plan.accessor.to_dict(),
                        )
                        futures.append(scan_future)
                for future in as_completed(futures):
                    scan_result = future.result()
                    scan_results.append(scan_result)
                logger.info(event=AWSLogEvents.ScanAWSAccountBatchEnd)
        return scan_results


def schedule_scan_account(
    executor: ThreadPoolExecutor,
    account_id: str,
    regions: List[str],
    resource_spec_classes: Tuple[Type[AWSResourceSpec], ...],
    graph_name: str,
    graph_version: str,
    max_svc_threads: int,
    artifact_writer: ArtifactWriter,
    accessor_dict: Dict[str, Any],
) -> Future:
    scan_lambda = lambda: scan_account(
        account_id=account_id,
        regions=regions,
        resource_spec_classes=resource_spec_classes,
        graph_name=graph_name,
        graph_version=graph_version,
        max_svc_threads=max_svc_threads,
        artifact_writer=artifact_writer,
        accessor_dict=accessor_dict,
    )
    return executor.submit(scan_lambda)


def scan_account(
    account_id: str,
    regions: List[str],
    resource_spec_classes: Tuple[Type[AWSResourceSpec], ...],
    graph_name: str,
    graph_version: str,
    max_svc_threads: int,
    artifact_writer: ArtifactWriter,
    accessor_dict: Dict[str, Any],
) -> Dict[str, Any]:
    """ FIXME TODO DOCUMENT ME """
    logger = Logger()
    logger.info(event=AWSLogEvents.ScanAWSAccountStart)
    stats = MultilevelCounter()
    errors: List[str] = []
    now = int(time.time())
    try:
        account_graph_set = GraphSet(
            name=graph_name,
            version=graph_version,
            start_time=now,
            end_time=now,
            resources=[],
            errors=[],
            stats=stats,
        )
        # sanity check
        accessor = Accessor.from_dict(data=accessor_dict)
        session = accessor.get_session(account_id=account_id)
        sts_client = session.client("sts")
        sts_account_id = sts_client.get_caller_identity()["Account"]
        if sts_account_id != account_id:
            raise ValueError(f"BUG: sts detected account_id {sts_account_id} != {account_id}")
        if regions:
            scan_regions = tuple(regions)
        else:
            scan_regions = get_all_enabled_regions(session=session)
        # build graph specs.
        # build a dict of regions -> services -> List[AWSResourceSpec]
        regions_services_resource_spec_classes: DefaultDict[
            str, DefaultDict[str, List[Type[AWSResourceSpec]]]
        ] = defaultdict(lambda: defaultdict(list))
        resource_spec_class: Type[AWSResourceSpec]
        for resource_spec_class in resource_spec_classes:
            client_name = resource_spec_class.get_client_name()
            if resource_spec_class.region_whitelist:
                resource_scan_regions = tuple(
                    region
                    for region in scan_regions
                    if region in resource_spec_class.region_whitelist
                )
                if not resource_scan_regions:
                    resource_scan_regions = resource_spec_class.region_whitelist
            else:
                resource_scan_regions = scan_regions
            if resource_spec_class.scan_granularity == ScanGranularity.ACCOUNT:
                regions_services_resource_spec_classes[resource_scan_regions[0]][
                    client_name
                ].append(resource_spec_class)
            elif resource_spec_class.scan_granularity == ScanGranularity.REGION:
                for region in resource_scan_regions:
                    regions_services_resource_spec_classes[region][client_name].append(
                        resource_spec_class
                    )
            else:
                raise NotImplementedError(
                    f"ScanGranularity {resource_spec_class.scan_granularity} unimplemented"
                )
        with ThreadPoolExecutor(max_workers=max_svc_threads) as executor:
            futures = []
            for (
                region,
                services_resource_spec_classes,
            ) in regions_services_resource_spec_classes.items():
                for (service, svc_resource_spec_classes,) in services_resource_spec_classes.items():
                    region_session = accessor.get_session(account_id=account_id, region_name=region)
                    region_creds = region_session.get_credentials()
                    scan_future = schedule_scan_service(
                        executor=executor,
                        graph_name=graph_name,
                        graph_version=graph_version,
                        account_id=account_id,
                        region=region,
                        service=service,
                        access_key=region_creds.access_key,
                        secret_key=region_creds.secret_key,
                        token=region_creds.token,
                        resource_spec_classes=tuple(svc_resource_spec_classes),
                    )
                    futures.append(scan_future)
            for future in as_completed(futures):
                graph_set_dict = future.result()
                graph_set = GraphSet.from_dict(graph_set_dict)
                errors += graph_set.errors
                account_graph_set.merge(graph_set)
            account_graph_set.validate()
    except Exception as ex:
        error_str = str(ex)
        trace_back = traceback.format_exc()
        logger.error(
            event=AWSLogEvents.ScanAWSAccountError, error=error_str, trace_back=trace_back,
        )
        errors.append(" : ".join((error_str, trace_back)))
        unscanned_account_resource = UnscannedAccountResourceSpec.create_resource(
            account_id=account_id, errors=errors
        )
        account_graph_set = GraphSet(
            name=graph_name,
            version=graph_version,
            start_time=now,
            end_time=now,
            resources=[unscanned_account_resource],
            errors=errors,
            stats=stats,
        )
        account_graph_set.validate()
    output_artifact = artifact_writer.write_artifact(
        name=account_id, data=account_graph_set.to_dict()
    )
    logger.info(event=AWSLogEvents.ScanAWSAccountEnd)
    api_call_stats = account_graph_set.stats.to_dict()
    return {
        "account_id": account_id,
        "output_artifact": output_artifact,
        "errors": errors,
        "api_call_stats": api_call_stats,
    }


def schedule_scan_service(
    executor: ThreadPoolExecutor,
    graph_name: str,
    graph_version: str,
    account_id: str,
    region: str,
    service: str,
    access_key: str,
    secret_key: str,
    token: str,
    resource_spec_classes: Tuple[Type[AWSResourceSpec], ...],
) -> Future:
    scan_lambda = lambda: scan_service(
        graph_name=graph_name,
        graph_version=graph_version,
        account_id=account_id,
        region=region,
        service=service,
        access_key=access_key,
        secret_key=secret_key,
        token=token,
        resource_spec_classes=resource_spec_classes,
    )
    return executor.submit(scan_lambda)


def scan_service(
    graph_name: str,
    graph_version: str,
    account_id: str,
    region: str,
    service: str,
    access_key: str,
    secret_key: str,
    token: str,
    resource_spec_classes: Tuple[Type[AWSResourceSpec], ...],
) -> Dict[str, Any]:
    logger = Logger()
    with logger.bind(region=region, service=service):
        logger.info(event=AWSLogEvents.ScanAWSAccountServiceStart)
        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=token,
            region_name=region,
        )
        aws_accessor = AWSAccessor(session=session, account_id=account_id, region_name=region)
        graph_spec = GraphSpec(
            name=graph_name,
            version=graph_version,
            resource_spec_classes=resource_spec_classes,
            scan_accessor=aws_accessor,
        )
        graph_set = graph_spec.scan()
        graph_set_dict = graph_set.to_dict()
        logger.info(event=AWSLogEvents.ScanAWSAccountServiceEnd)
        return graph_set_dict
