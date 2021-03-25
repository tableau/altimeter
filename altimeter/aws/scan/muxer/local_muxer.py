"""AWSScanMuxer that runs account scans one-per-thread"""
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Tuple, Type

from altimeter.aws.resource.resource_spec import AWSResourceSpec
from altimeter.aws.scan.account_scanner import AccountScanner, AccountScanResult
from altimeter.aws.scan.scan_plan import AccountScanPlan
from altimeter.aws.scan.muxer import AWSScanMuxer
from altimeter.aws.scan.settings import DEFAULT_RESOURCE_SPEC_CLASSES
from altimeter.core.artifact_io.writer import ArtifactWriter
from altimeter.core.config import AWSConfig


def local_account_scan(
    scan_id: str,
    account_scan_plan: AccountScanPlan,
    config: AWSConfig,
    resource_spec_classes: Tuple[Type[AWSResourceSpec], ...],
) -> AccountScanResult:
    """Scan a set of accounts.

    Args:
        account_scan_plan_dict: AccountScanPlan defining the scan
        config: Config object
    """
    artifact_writer = ArtifactWriter.from_artifact_path(
        artifact_path=config.artifact_path, scan_id=scan_id
    )
    account_scanner = AccountScanner(
        account_scan_plan=account_scan_plan,
        artifact_writer=artifact_writer,
        max_svc_scan_threads=config.concurrency.max_svc_scan_threads,
        scan_sub_accounts=config.scan.scan_sub_accounts,
        resource_spec_classes=resource_spec_classes,
    )
    return account_scanner.scan()


class LocalAWSScanMuxer(AWSScanMuxer):
    """AWSScanMuxer that runs account scans batches of accounts using local os threads"""

    def __init__(
        self,
        scan_id: str,
        config: AWSConfig,
        resource_spec_classes: Tuple[Type[AWSResourceSpec], ...] = DEFAULT_RESOURCE_SPEC_CLASSES,
    ):
        super().__init__(scan_id=scan_id, config=config)
        self.resource_spec_classes = resource_spec_classes

    def _schedule_account_scan(
        self, executor: ThreadPoolExecutor, account_scan_plan: AccountScanPlan
    ) -> Future:
        """Schedule a local account scan. Note that we serialize the AccountScanPlan
        because boto3 sessions are not thread safe.

        Args:
            executor: ThreadPoolExecutor to submit scan to
            account_scan_plan: AccountScanPlan defining this scan
        """
        scan_lambda = lambda: local_account_scan(
            scan_id=self.scan_id,
            account_scan_plan=account_scan_plan,
            config=self.config,
            resource_spec_classes=self.resource_spec_classes,
        )
        return executor.submit(scan_lambda)
