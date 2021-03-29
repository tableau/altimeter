"""An AccountScanner scans a set of accounts using an AccountScanPlan to define scan
parameters"""
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import random
import time
import traceback
from typing import DefaultDict, Dict, List, Tuple, Type

import boto3

from altimeter.aws.log_events import AWSLogEvents
from altimeter.aws.resource.resource_spec import ScanGranularity, AWSResourceSpec
from altimeter.aws.resource.unscanned_account import UnscannedAccountResourceSpec
from altimeter.aws.scan.scan_plan import AccountScanPlan
from altimeter.aws.scan.aws_accessor import AWSAccessor
from altimeter.aws.scan.settings import (
    DEFAULT_RESOURCE_SPEC_CLASSES,
    INFRA_RESOURCE_SPEC_CLASSES,
    ORG_RESOURCE_SPEC_CLASSES,
)
from altimeter.aws.settings import (
    GRAPH_NAME,
    GRAPH_VERSION,
)
from altimeter.core.artifact_io.writer import ArtifactWriter
from altimeter.core.base_model import BaseImmutableModel
from altimeter.core.graph.graph_set import GraphSet
from altimeter.core.graph.graph_spec import GraphSpec
from altimeter.core.log import Logger
from altimeter.core.resource.resource import Resource


class AccountScanResult(BaseImmutableModel):
    """pydantic model representing account scan results """

    account_id: str
    artifacts: List[str]
    errors: List[str]


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
    """Represents a single unit of scan which can be performed concurrently alongside any other
    ScanUnit - in general ScanUnits should be organized to avoid API limits"""

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
        scan_sub_accounts: bool,
        graph_name: str = GRAPH_NAME,
        graph_version: str = GRAPH_VERSION,
        resource_spec_classes: Tuple[Type[AWSResourceSpec], ...] = DEFAULT_RESOURCE_SPEC_CLASSES,
    ) -> None:
        self.account_scan_plan = account_scan_plan
        self.artifact_writer = artifact_writer
        self.graph_name = graph_name
        self.graph_version = graph_version
        self.max_threads = max_svc_scan_threads
        self.resource_spec_classes = resource_spec_classes + INFRA_RESOURCE_SPEC_CLASSES
        if scan_sub_accounts:
            self.resource_spec_classes += ORG_RESOURCE_SPEC_CLASSES

    def scan(self) -> AccountScanResult:
        logger = Logger()
        now = int(time.time())
        prescan_errors: List[str] = []
        futures: List[Future] = []
        account_id = self.account_scan_plan.account_id
        with logger.bind(account_id=account_id):
            with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
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
                        account_scan_regions = tuple(self.account_scan_plan.regions)
                    else:
                        account_scan_regions = get_all_enabled_regions(session=session)
                    # build a dict of regions -> services -> List[AWSResourceSpec]
                    regions_services_resource_spec_classes: DefaultDict[
                        str, DefaultDict[str, List[Type[AWSResourceSpec]]]
                    ] = defaultdict(lambda: defaultdict(list))
                    for resource_spec_class in self.resource_spec_classes:
                        resource_regions = self.account_scan_plan.aws_resource_region_mapping_repo.get_regions(
                            resource_spec_class=resource_spec_class,
                            region_whitelist=account_scan_regions,
                        )
                        for region in resource_regions:
                            regions_services_resource_spec_classes[region][
                                resource_spec_class.service_name
                            ].append(resource_spec_class)
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
                    prescan_errors.append(f"{error_str}\n{trace_back}")
            graph_sets: List[GraphSet] = []
            for future in as_completed(futures):
                graph_set = future.result()
                graph_sets.append(graph_set)
            # if there was a prescan error graph it and return the result
            if prescan_errors:
                unscanned_account_resource = UnscannedAccountResourceSpec.create_resource(
                    account_id=account_id, errors=prescan_errors
                )
                account_graph_set = GraphSet(
                    name=self.graph_name,
                    version=self.graph_version,
                    start_time=now,
                    end_time=now,
                    resources=[unscanned_account_resource],
                    errors=prescan_errors,
                )
                output_artifact = self.artifact_writer.write_json(
                    name=account_id, data=account_graph_set,
                )
                logger.info(event=AWSLogEvents.ScanAWSAccountEnd)
                return AccountScanResult(
                    account_id=account_id, artifacts=[output_artifact], errors=prescan_errors,
                )
            # if there are any errors whatsoever we generate an empty graph with errors only
            errors = []
            for graph_set in graph_sets:
                errors += graph_set.errors
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
                )
            else:
                account_graph_set = GraphSet.from_graph_sets(graph_sets)
            output_artifact = self.artifact_writer.write_json(
                name=account_id, data=account_graph_set,
            )
            logger.info(event=AWSLogEvents.ScanAWSAccountEnd)
            return AccountScanResult(
                account_id=account_id, artifacts=[output_artifact], errors=errors,
            )


def scan_scan_unit(scan_unit: ScanUnit) -> GraphSet:
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
        )
        end_t = time.time()
        elapsed_sec = end_t - start_t
        logger.info(event=AWSLogEvents.ScanAWSAccountServiceEnd, elapsed_sec=elapsed_sec)
        return graph_set


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
