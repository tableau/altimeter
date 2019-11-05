"""An AccountScanner scans a single account using an AccountScanPlan to define scan
parameters"""
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
import time
import traceback
from typing import Any, DefaultDict, Dict, List, Tuple, Type

import boto3

from altimeter.core.artifact_io.writer import ArtifactWriter
from altimeter.aws.log import AWSLogEvents
from altimeter.aws.resource.resource_spec import ScanGranularity, AWSResourceSpec
from altimeter.aws.resource.unscanned_account import UnscannedAccountResourceSpec
from altimeter.aws.scan.account_scan_plan import AccountScanPlan
from altimeter.aws.scan.aws_accessor import AWSAccessor
from altimeter.aws.scan.settings import (
    RESOURCE_SPEC_CLASSES,
    INFRA_RESOURCE_SPEC_CLASSES,
    ORG_RESOURCE_SPEC_CLASSES,
)
from altimeter.aws.settings import GRAPH_NAME, GRAPH_VERSION
from altimeter.core.graph.graph_set import GraphSet
from altimeter.core.graph.graph_spec import GraphSpec
from altimeter.core.log import Logger
from altimeter.core.multilevel_counter import MultilevelCounter

DEFAULT_MAX_SVC_THREADS = 16


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


class AccountScanner:
    """An AccountScanner scans a single account using an AccountScanPlan to define scan parameters
    and writes the output using an ArtifactWriter.

    Args:
        account_scan_plan: AccountScanPlan to scan
        artifact_writer: ArtifactWriter for writing out artifacts
        scan_sub_accounts: if set to True, if this account is an org master any subaccounts
                           of that org will also be scanned.
        graph_name: name of graph
        graph_version: version string for graph
        max_svc_threads: max number of scan threads to run concurrently.
    """

    def __init__(
        self,
        account_scan_plan: AccountScanPlan,
        artifact_writer: ArtifactWriter,
        scan_sub_accounts: bool,
        max_svc_threads: int,
        graph_name: str = GRAPH_NAME,
        graph_version: str = GRAPH_VERSION,
    ) -> None:
        self.account_id = account_scan_plan.account_id
        self.artifact_writer = artifact_writer
        self.max_svc_threads = max_svc_threads
        self.regions = account_scan_plan.regions
        self.account_scan_plan = account_scan_plan
        self.get_session = account_scan_plan.get_session
        self.graph_name = graph_name
        self.graph_version = graph_version
        self.resource_spec_classes = RESOURCE_SPEC_CLASSES + INFRA_RESOURCE_SPEC_CLASSES
        if scan_sub_accounts:
            self.resource_spec_classes += ORG_RESOURCE_SPEC_CLASSES

    def scan(self) -> Dict[str, Any]:
        """Scan an account and return a dict containing keys:

            * account_id: str
            * output_artifact: str
            * api_call_stats: Dict[str, Any]
            * errors: List[str]

        If errors is non-empty the results are incomplete for this account.
        output_artifact is a pointer to the actual scan data - either on the local fs or in s3.

        To scan an account we create a set of GraphSpecs, one for each region.  Any ACCOUNT
        level granularity resources are only scanned in a single region (e.g. IAM Users)

        Returns:
            Dict of scan result, see above for details.
        """
        logger = Logger()
        with logger.bind(account_id=self.account_id):
            logger.info(event=AWSLogEvents.ScanAWSAccountStart)
            output_artifact = None
            stats = MultilevelCounter()
            errors: List[str] = []
            now = int(time.time())
            account_graph_set = GraphSet(
                name=self.graph_name,
                version=self.graph_version,
                start_time=now,
                end_time=now,
                resources=[],
                errors=[],
                stats=stats,
            )
            try:
                # sanity check
                session = self.get_session()
                sts_client = session.client("sts")
                sts_account_id = sts_client.get_caller_identity()["Account"]
                if sts_account_id != self.account_id:
                    raise ValueError(
                        f"BUG: sts detected account_id {sts_account_id} != {self.account_id}"
                    )
                if self.regions:
                    scan_regions = tuple(self.regions)
                else:
                    scan_regions = get_all_enabled_regions(session=session)
                # build graph specs.
                # build a dict of regions -> services -> List[AWSResourceSpec]
                regions_services_resource_spec_classes: DefaultDict[
                    str, DefaultDict[str, List[Type[AWSResourceSpec]]]
                ] = defaultdict(lambda: defaultdict(list))
                resource_spec_class: Type[AWSResourceSpec]
                for resource_spec_class in self.resource_spec_classes:
                    client_name = resource_spec_class.get_client_name()
                    resource_class_scan_granularity = resource_spec_class.scan_granularity
                    if resource_class_scan_granularity == ScanGranularity.ACCOUNT:
                        regions_services_resource_spec_classes[scan_regions[0]][client_name].append(
                            resource_spec_class
                        )
                    elif resource_class_scan_granularity == ScanGranularity.REGION:
                        for region in scan_regions:
                            regions_services_resource_spec_classes[region][client_name].append(
                                resource_spec_class
                            )
                    else:
                        raise NotImplementedError(
                            f"ScanGranularity {resource_class_scan_granularity} not implemented"
                        )
                with ThreadPoolExecutor(max_workers=self.max_svc_threads) as executor:
                    futures = []
                    for (
                        region,
                        services_resource_spec_classes,
                    ) in regions_services_resource_spec_classes.items():
                        for (
                            service,
                            resource_spec_classes,
                        ) in services_resource_spec_classes.items():
                            region_session = self.get_session(region=region)
                            region_creds = region_session.get_credentials()
                            scan_future = schedule_scan_services(
                                executor=executor,
                                graph_name=self.graph_name,
                                graph_version=self.graph_version,
                                account_id=self.account_id,
                                region=region,
                                service=service,
                                access_key=region_creds.access_key,
                                secret_key=region_creds.secret_key,
                                token=region_creds.token,
                                resource_spec_classes=tuple(resource_spec_classes),
                            )
                            futures.append(scan_future)
                    for future in as_completed(futures):
                        graph_set_dict = future.result()
                        graph_set = GraphSet.from_dict(graph_set_dict)
                        errors += graph_set.errors
                        account_graph_set.merge(graph_set)
                account_graph_set.validate()
                output_artifact = self.artifact_writer.write_artifact(
                    name=self.account_id, data=account_graph_set.to_dict()
                )
                logger.info(event=AWSLogEvents.ScanAWSAccountEnd)
            except Exception as ex:
                error_str = str(ex)
                trace_back = traceback.format_exc()
                logger.error(
                    event=AWSLogEvents.ScanAWSAccountError, error=error_str, trace_back=trace_back
                )
                errors.append(" : ".join((error_str, trace_back)))
                unscanned_account_resource = UnscannedAccountResourceSpec.create_resource(
                    account_id=self.account_id, errors=errors
                )
                failed_account_graph_set = GraphSet(
                    name=self.graph_name,
                    version=self.graph_version,
                    start_time=now,
                    end_time=now,
                    resources=[unscanned_account_resource],
                    errors=errors,
                    stats=stats,
                )
                account_graph_set.merge(failed_account_graph_set)
        api_call_stats = account_graph_set.stats.to_dict()
        return {
            "account_id": self.account_id,
            "output_artifact": output_artifact,
            "errors": errors,
            "api_call_stats": api_call_stats,
        }


def schedule_scan_services(
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
    scan_lambda = lambda: scan_services(
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


def scan_services(
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
        with logger.bind(region=region, service=service):
            graph_set = graph_spec.scan()
            graph_set_dict = graph_set.to_dict()
            logger.info(event=AWSLogEvents.ScanAWSAccountServiceEnd)
            return graph_set_dict
