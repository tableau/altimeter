"""Abstract base class for AWSScanMuxers."""
import abc
from concurrent.futures import as_completed, Future, ThreadPoolExecutor
from typing import List

from altimeter.core.log import Logger
from altimeter.aws.log import AWSLogEvents
from altimeter.aws.scan.account_scan_plan import AccountScanPlan
from altimeter.aws.scan.account_scan_manifest import AccountScanManifest


class AWSScanMuxer(abc.ABC):
    """Abstract base class for AWSScanMuxers.  AWSScanMuxers coordinate multi-account scans
    across a pool of threads which either call account scan code locally in the case of
    a local run or invoke a Lambda-per-account in the case of Altimeter running on AWS Lambda.

    Args:
        max_threads: maximum number of threads to allow concurrently.
    """

    def __init__(self, max_threads: int):
        self.max_threads = max_threads

    def scan(self, account_scan_plans: List[AccountScanPlan]) -> List[AccountScanManifest]:
        """Scan accounts. Return a list of AccountScanManifest objects.

        Args:
            account_scan_plans: list of AccountScanPlan objects defining this scan op

        Returns:
            list of AccountScanManifest objects describing the output of the scan.
        """
        account_scan_results: List[AccountScanManifest] = []
        num_total_accounts = len(account_scan_plans)
        num_threads = min(num_total_accounts, self.max_threads)
        logger = Logger()
        with logger.bind(
            num_total_accounts=num_total_accounts,
            muxer=self.__class__.__name__,
            num_threads=num_threads,
        ):
            logger.info(event=AWSLogEvents.MuxerStart)
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                processed_accounts = 0
                futures = []
                for account_scan_plan in account_scan_plans:
                    account_scan_future = self._schedule_account_scan(executor, account_scan_plan)
                    futures.append(account_scan_future)
                    logger.info(
                        event=AWSLogEvents.MuxerQueueScan, account_id=account_scan_plan.account_id
                    )
                for future in as_completed(futures):
                    scan_results_dict = future.result()
                    account_id = scan_results_dict["account_id"]
                    output_artifact = scan_results_dict["output_artifact"]
                    account_errors = scan_results_dict["errors"]
                    api_call_stats = scan_results_dict["api_call_stats"]
                    artifacts = [output_artifact] if output_artifact else []
                    account_scan_result = AccountScanManifest(
                        account_id=account_id,
                        artifacts=artifacts,
                        errors=account_errors,
                        api_call_stats=api_call_stats,
                    )
                    account_scan_results.append(account_scan_result)
                    processed_accounts += 1
                    logger.info(event=AWSLogEvents.MuxerStat, num_scanned=processed_accounts)
            logger.info(event=AWSLogEvents.MuxerEnd)
        return account_scan_results

    @abc.abstractmethod
    def _schedule_account_scan(
        self, executor: ThreadPoolExecutor, account_scan_plan: AccountScanPlan
    ) -> Future:
        """Given a ThreadPoolExecutor and scan details (date/time/account/regions),
        schedule an account scan by making a call to executor.submit and return the Future
        returned by executor.submit.

        In a local environment this means creating an AccountScanner directly and
        calling executor.submit(AccountScanner.scan)
        """
