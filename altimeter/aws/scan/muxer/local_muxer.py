"""AWSScanMuxer that runs account scans one-per-thread"""
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Dict, List

from altimeter.aws.scan.muxer import AWSScanMuxer
from altimeter.aws.scan.account_scan_plan import AccountScanPlan
from altimeter.aws.scan.account_scanner import AccountScanner
from altimeter.core.artifact_io.writer import ArtifactWriter
from altimeter.core.config import Config


def local_account_scan(
    scan_id: str, account_scan_plan_dict: Dict[str, Any], config: Config,
) -> List[Dict[str, Any]]:
    """Scan a set of accounts.

    Args:
        account_scan_plan_dict: AccountScanPlan defining the scan
        config: Config object
    """
    artifact_writer = ArtifactWriter.from_artifact_path(
        artifact_path=config.artifact_path, scan_id=scan_id
    )
    account_scan_plan = AccountScanPlan.from_dict(account_scan_plan_dict=account_scan_plan_dict)
    account_scanner = AccountScanner(
        account_scan_plan=account_scan_plan,
        artifact_writer=artifact_writer,
        max_svc_scan_threads=config.concurrency.max_svc_scan_threads,
        preferred_account_scan_regions=config.scan.preferred_account_scan_regions,
        scan_sub_accounts=config.scan.scan_sub_accounts,
    )
    return account_scanner.scan()


class LocalAWSScanMuxer(AWSScanMuxer):
    """AWSScanMuxer that runs account scans batches of accounts using local os threads"""

    def _schedule_account_scan(
        self, executor: ThreadPoolExecutor, account_scan_plan: AccountScanPlan
    ) -> Future:
        """Schedule a local account scan. Note that we serialize the AccountScanPlan
        because boto3 sessions are not thread safe.

        Args:
            executor: ThreadPoolExecutor to submit scan to
            account_scan_plan: AccountScanPlans defining this scan
        """
        scan_lambda = lambda: local_account_scan(
            scan_id=self.scan_id,
            account_scan_plan_dict=account_scan_plan.to_dict(),
            config=self.config,
        )
        return executor.submit(scan_lambda)
