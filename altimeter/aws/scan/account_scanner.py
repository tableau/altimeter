"""An AccountScanner scans a single account using an AccountScanPlan to define scan
parameters"""
from typing import List

from altimeter.core.artifact_io.writer import ArtifactWriter
from altimeter.aws.scan.settings import (
    RESOURCE_SPEC_CLASSES,
    INFRA_RESOURCE_SPEC_CLASSES,
    ORG_RESOURCE_SPEC_CLASSES,
)
from altimeter.aws.settings import GRAPH_NAME, GRAPH_VERSION
from altimeter.aws.scan.base_scanner import BaseScanner, GetSessionType


class AccountScanner(BaseScanner):  # pylint: disable=too-few-public-methods
    """An AccountScanner scans a single account using an AccountScanPlan to define scan parameters
    and writes the output using an ArtifactWriter.

    Args:
        account_id: account id to scan
        regions: regions to scan
        get_session: callable that can get a session in this account_id
        artifact_writer: ArtifactWriter for writing out artifacts
        scan_sub_accounts: if set to True, if this account is an org master any subaccounts
                           of that org will also be scanned.
        graph_name: name of graph
        graph_version: version string for graph
        max_svc_threads: max number of scan threads to run concurrently.
    """

    def __init__(
        self,
        account_id: str,
        regions: List[str],
        get_session: GetSessionType,
        artifact_writer: ArtifactWriter,
        scan_sub_accounts: bool,
        max_svc_threads: int,
        graph_name: str = GRAPH_NAME,
        graph_version: str = GRAPH_VERSION,
    ) -> None:
        resource_spec_classes = RESOURCE_SPEC_CLASSES + INFRA_RESOURCE_SPEC_CLASSES
        if scan_sub_accounts:
            resource_spec_classes += ORG_RESOURCE_SPEC_CLASSES
        super().__init__(
            account_id=account_id,
            regions=regions,
            get_session=get_session,
            artifact_writer=artifact_writer,
            max_svc_threads=max_svc_threads,
            graph_name=graph_name,
            graph_version=graph_version,
            resource_spec_classes=resource_spec_classes,
        )
