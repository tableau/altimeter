"""AWSScanMuxer that runs account scans one-per-thread"""
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List

from altimeter.aws.scan.muxer import AWSScanMuxer
from altimeter.aws.scan.account_scan_plan import AccountScanPlan
from altimeter.aws.scan.account_scanner import AccountScanner
from altimeter.core.artifact_io.writer import FileArtifactWriter
from altimeter.core.config import Config


def local_account_scan(
    account_scan_plan_dict: Dict[str, Any], config: Config, output_dir: Path,
) -> List[Dict[str, Any]]:
    """Scan a set of accounts.

    Args:
        account_scan_plan_dict: AccountScanPlan defining the scan
        config: Config object
        output_dir: output artifats to this Path
    """
    artifact_writer = FileArtifactWriter(output_dir=output_dir)
    account_scan_plan = AccountScanPlan.from_dict(account_scan_plan_dict=account_scan_plan_dict)
    account_scanner = AccountScanner(
        account_scan_plan=account_scan_plan, artifact_writer=artifact_writer, config=config,
    )
    return account_scanner.scan()


class LocalAWSScanMuxer(AWSScanMuxer):
    """AWSScanMuxer that runs account scans one account per thread and saves results
    to a filesystem path.

    Args:
        output_dir: output artifacts to this dir
        config: Config object
    """

    def __init__(
        self, output_dir: Path, config: Config,
    ):
        super().__init__(config=config)
        self.output_dir = output_dir

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
            account_scan_plan_dict=account_scan_plan.to_dict(),
            output_dir=self.output_dir,
            config=self.config,
        )
        return executor.submit(scan_lambda)
