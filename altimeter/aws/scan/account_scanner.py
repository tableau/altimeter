"""An AccountScanner scans a set of accounts using an AccountScanPlan to define scan
parameters"""
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import random
import time
import traceback
from typing import Any, DefaultDict, Dict, List, Tuple, Type

import boto3

from altimeter.aws.log_events import AWSLogEvents
from altimeter.aws.resource.resource_spec import ScanGranularity, AWSResourceSpec
from altimeter.aws.resource.unscanned_account import UnscannedAccountResourceSpec
from altimeter.aws.scan.account_scan_plan import AccountScanPlan
from altimeter.aws.scan.aws_accessor import AWSAccessor
from altimeter.aws.scan.settings import (
    RESOURCE_SPEC_CLASSES,
    INFRA_RESOURCE_SPEC_CLASSES,
    ORG_RESOURCE_SPEC_CLASSES,
)
from altimeter.aws.settings import (
    GRAPH_NAME,
    GRAPH_VERSION,
)
from altimeter.core.artifact_io.writer import ArtifactWriter
from altimeter.core.graph.graph_set import GraphSet
from altimeter.core.graph.graph_spec import GraphSpec
from altimeter.core.log import Logger
from altimeter.core.multilevel_counter import MultilevelCounter
from altimeter.core.resource.resource import Resource


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


@dataclass(frozen=True)
class ScanUnit:
    graph_name: str
    graph_version: str
    account_id: str
    region_name: str
    service: str
    access_key: str
    secret_key: str
    token: str
    resource_spec_classes: Tuple[Type[AWSResourceSpec], ...]


class AccountScanner:
    """An AccountScanner scans a set of accounts using an AccountScanPlan to define scan
    parameters

    Args:
        account_scan_plan: AccountScanPlan describing scan targets
        artifact_writer: ArtifactWriter for writing out artifacts
        graph_name: name of graph
        graph_version: version string for graph
    """

    def __init__(
        self,
        account_scan_plan: AccountScanPlan,
        artifact_writer: ArtifactWriter,
        max_svc_scan_threads: int,
        preferred_account_scan_regions: Tuple[str, ...],
        scan_sub_accounts: bool,
        graph_name: str = GRAPH_NAME,
        graph_version: str = GRAPH_VERSION,
    ) -> None:
        self.account_scan_plan = account_scan_plan
        self.artifact_writer = artifact_writer
        self.graph_name = graph_name
        self.graph_version = graph_version
        self.max_threads = max_svc_scan_threads
        self.preferred_account_scan_regions = preferred_account_scan_regions
        self.resource_spec_classes = RESOURCE_SPEC_CLASSES + INFRA_RESOURCE_SPEC_CLASSES
        if scan_sub_accounts:
            self.resource_spec_classes += ORG_RESOURCE_SPEC_CLASSES

    def scan(self) -> List[Dict[str, Any]]:
        logger = Logger()
        scan_result_dicts = []
        now = int(time.time())
        prescan_account_ids_errors: DefaultDict[str, List[str]] = defaultdict(list)
        futures = []
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            shuffled_account_ids = random.sample(
                self.account_scan_plan.account_ids, k=len(self.account_scan_plan.account_ids)
            )
            for account_id in shuffled_account_ids:
                with logger.bind(account_id=account_id):
                    logger.info(event=AWSLogEvents.ScanAWSAccountStart)
                    try:
                        session = self.account_scan_plan.accessor.get_session(account_id=account_id)
                        # sanity check
                        sts_client = session.client("sts")
                        sts_account_id = sts_client.get_caller_identity()["Account"]
                        if sts_account_id != account_id:
                            raise ValueError(
                                f"BUG: sts detected account_id {sts_account_id} != {account_id}"
                            )
                        if self.account_scan_plan.regions:
                            scan_regions = tuple(self.account_scan_plan.regions)
                        else:
                            scan_regions = get_all_enabled_regions(session=session)
                        account_gran_scan_region = random.choice(
                            self.preferred_account_scan_regions
                        )
                        # build a dict of regions -> services -> List[AWSResourceSpec]
                        regions_services_resource_spec_classes: DefaultDict[
                            str, DefaultDict[str, List[Type[AWSResourceSpec]]]
                        ] = defaultdict(lambda: defaultdict(list))
                        resource_spec_class: Type[AWSResourceSpec]
                        for resource_spec_class in self.resource_spec_classes:
                            client_name = resource_spec_class.get_client_name()
                            if resource_spec_class.scan_granularity == ScanGranularity.ACCOUNT:
                                if resource_spec_class.region_whitelist:
                                    account_resource_scan_region = resource_spec_class.region_whitelist[
                                        0
                                    ]
                                else:
                                    account_resource_scan_region = account_gran_scan_region
                                regions_services_resource_spec_classes[
                                    account_resource_scan_region
                                ][client_name].append(resource_spec_class)
                            elif resource_spec_class.scan_granularity == ScanGranularity.REGION:
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
                                for region in resource_scan_regions:
                                    regions_services_resource_spec_classes[region][
                                        client_name
                                    ].append(resource_spec_class)
                            else:
                                raise NotImplementedError(
                                    f"ScanGranularity {resource_spec_class.scan_granularity} unimplemented"
                                )
                        # Build and submit ScanUnits
                        shuffed_regions_services_resource_spec_classes = random.sample(
                            regions_services_resource_spec_classes.items(),
                            len(regions_services_resource_spec_classes),
                        )
                        for (
                            region,
                            services_resource_spec_classes,
                        ) in shuffed_regions_services_resource_spec_classes:
                            region_session = self.account_scan_plan.accessor.get_session(
                                account_id=account_id, region_name=region
                            )
                            region_creds = region_session.get_credentials()
                            shuffled_services_resource_spec_classes = random.sample(
                                services_resource_spec_classes.items(),
                                len(services_resource_spec_classes),
                            )
                            for (
                                service,
                                svc_resource_spec_classes,
                            ) in shuffled_services_resource_spec_classes:
                                parallel_svc_resource_spec_classes = [
                                    svc_resource_spec_class
                                    for svc_resource_spec_class in svc_resource_spec_classes
                                    if svc_resource_spec_class.parallel_scan
                                ]
                                serial_svc_resource_spec_classes = [
                                    svc_resource_spec_class
                                    for svc_resource_spec_class in svc_resource_spec_classes
                                    if not svc_resource_spec_class.parallel_scan
                                ]
                                for (
                                    parallel_svc_resource_spec_class
                                ) in parallel_svc_resource_spec_classes:
                                    parallel_future = schedule_scan(
                                        executor=executor,
                                        graph_name=self.graph_name,
                                        graph_version=self.graph_version,
                                        account_id=account_id,
                                        region_name=region,
                                        service=service,
                                        access_key=region_creds.access_key,
                                        secret_key=region_creds.secret_key,
                                        token=region_creds.token,
                                        resource_spec_classes=(parallel_svc_resource_spec_class,),
                                    )
                                    futures.append(parallel_future)
                                serial_future = schedule_scan(
                                    executor=executor,
                                    graph_name=self.graph_name,
                                    graph_version=self.graph_version,
                                    account_id=account_id,
                                    region_name=region,
                                    service=service,
                                    access_key=region_creds.access_key,
                                    secret_key=region_creds.secret_key,
                                    token=region_creds.token,
                                    resource_spec_classes=tuple(serial_svc_resource_spec_classes),
                                )
                                futures.append(serial_future)
                    except Exception as ex:
                        error_str = str(ex)
                        trace_back = traceback.format_exc()
                        logger.error(
                            event=AWSLogEvents.ScanAWSAccountError,
                            error=error_str,
                            trace_back=trace_back,
                        )
                        prescan_account_ids_errors[account_id].append(f"{error_str}\n{trace_back}")
        account_ids_graph_set_dicts: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for future in as_completed(futures):
            account_id, graph_set_dict = future.result()
            account_ids_graph_set_dicts[account_id].append(graph_set_dict)
        # first make sure no account id appears both in account_ids_graph_set_dicts
        # and prescan_account_ids_errors - this should never happen
        doubled_accounts = set(account_ids_graph_set_dicts.keys()).intersection(
            set(prescan_account_ids_errors.keys())
        )
        if doubled_accounts:
            raise Exception(
                (
                    f"BUG: Account(s) {doubled_accounts} in both "
                    "account_ids_graph_set_dicts and prescan_account_ids_errors."
                )
            )
        # graph prescan error accounts
        for account_id, errors in prescan_account_ids_errors.items():
            with logger.bind(account_id=account_id):
                unscanned_account_resource = UnscannedAccountResourceSpec.create_resource(
                    account_id=account_id, errors=errors
                )
                account_graph_set = GraphSet(
                    name=self.graph_name,
                    version=self.graph_version,
                    start_time=now,
                    end_time=now,
                    resources=[unscanned_account_resource],
                    errors=errors,
                    stats=MultilevelCounter(),
                )
                account_graph_set.validate()
                output_artifact = self.artifact_writer.write_json(
                    name=account_id, data=account_graph_set.to_dict()
                )
                logger.info(event=AWSLogEvents.ScanAWSAccountEnd)
                api_call_stats = account_graph_set.stats.to_dict()
                scan_result_dicts.append(
                    {
                        "account_id": account_id,
                        "output_artifact": output_artifact,
                        "errors": errors,
                        "api_call_stats": api_call_stats,
                    }
                )
        # graph rest
        for account_id, graph_set_dicts in account_ids_graph_set_dicts.items():
            with logger.bind(account_id=account_id):
                # if there are any errors whatsoever we generate an empty graph with
                # errors only
                errors = []
                for graph_set_dict in graph_set_dicts:
                    errors += graph_set_dict["errors"]
                if errors:
                    unscanned_account_resource = UnscannedAccountResourceSpec.create_resource(
                        account_id=account_id, errors=errors
                    )
                    account_graph_set = GraphSet(
                        name=self.graph_name,
                        version=self.graph_version,
                        start_time=now,
                        end_time=now,
                        resources=[unscanned_account_resource],
                        errors=errors,
                        stats=MultilevelCounter(),  # ENHANCHMENT: could technically get partial stats.
                    )
                    account_graph_set.validate()
                else:
                    account_graph_set = GraphSet(
                        name=self.graph_name,
                        version=self.graph_version,
                        start_time=now,
                        end_time=now,
                        resources=[],
                        errors=[],
                        stats=MultilevelCounter(),
                    )
                    for graph_set_dict in graph_set_dicts:
                        graph_set = GraphSet.from_dict(graph_set_dict)
                        account_graph_set.merge(graph_set)
                output_artifact = self.artifact_writer.write_json(
                    name=account_id, data=account_graph_set.to_dict()
                )
                logger.info(event=AWSLogEvents.ScanAWSAccountEnd)
                api_call_stats = account_graph_set.stats.to_dict()
                scan_result_dicts.append(
                    {
                        "account_id": account_id,
                        "output_artifact": output_artifact,
                        "errors": errors,
                        "api_call_stats": api_call_stats,
                    }
                )
        return scan_result_dicts


def scan_scan_unit(scan_unit: ScanUnit) -> Tuple[str, Dict[str, Any]]:
    logger = Logger()
    with logger.bind(
        account_id=scan_unit.account_id,
        region=scan_unit.region_name,
        service=scan_unit.service,
        resource_classes=sorted(
            [
                resource_spec_class.__name__
                for resource_spec_class in scan_unit.resource_spec_classes
            ]
        ),
    ):
        start_t = time.time()
        logger.info(event=AWSLogEvents.ScanAWSAccountServiceStart)
        session = boto3.Session(
            aws_access_key_id=scan_unit.access_key,
            aws_secret_access_key=scan_unit.secret_key,
            aws_session_token=scan_unit.token,
            region_name=scan_unit.region_name,
        )
        scan_accessor = AWSAccessor(
            session=session, account_id=scan_unit.account_id, region_name=scan_unit.region_name
        )
        graph_spec = GraphSpec(
            name=scan_unit.graph_name,
            version=scan_unit.graph_version,
            resource_spec_classes=scan_unit.resource_spec_classes,
            scan_accessor=scan_accessor,
        )
        start_time = int(time.time())
        resources: List[Resource] = []
        errors = []
        try:
            resources = graph_spec.scan()
        except Exception as ex:
            error_str = str(ex)
            trace_back = traceback.format_exc()
            logger.error(
                event=AWSLogEvents.ScanAWSAccountError, error=error_str, trace_back=trace_back
            )
            error = f"{str(ex)}\n{trace_back}"
            errors.append(error)
        end_time = int(time.time())
        graph_set = GraphSet(
            name=scan_unit.graph_name,
            version=scan_unit.graph_version,
            start_time=start_time,
            end_time=end_time,
            resources=resources,
            errors=errors,
            stats=scan_accessor.api_call_stats,
        )
        end_t = time.time()
        elapsed_sec = end_t - start_t
        logger.info(event=AWSLogEvents.ScanAWSAccountServiceEnd, elapsed_sec=elapsed_sec)
        return (scan_unit.account_id, graph_set.to_dict())


def schedule_scan(
    executor: ThreadPoolExecutor,
    graph_name: str,
    graph_version: str,
    account_id: str,
    region_name: str,
    service: str,
    access_key: str,
    secret_key: str,
    token: str,
    resource_spec_classes: Tuple[Type[AWSResourceSpec], ...],
) -> Future:
    scan_unit = ScanUnit(
        graph_name=graph_name,
        graph_version=graph_version,
        account_id=account_id,
        region_name=region_name,
        service=service,
        access_key=access_key,
        secret_key=secret_key,
        token=token,
        resource_spec_classes=resource_spec_classes,
    )
    future = executor.submit(lambda: scan_scan_unit(scan_unit=scan_unit))
    return future
