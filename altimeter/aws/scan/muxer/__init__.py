"""Abstract base class for AWSScanMuxers."""
import abc
from concurrent.futures import as_completed, Future, ThreadPoolExecutor
from typing import Generator

from altimeter.aws.log_events import AWSLogEvents
from altimeter.aws.scan.account_scan_plan import AccountScanPlan
from altimeter.aws.scan.account_scan_manifest import AccountScanManifest
from altimeter.core.config import Config
from altimeter.core.log import Logger


class AWSScanMuxer(abc.ABC):
    """Abstract base class for AWSScanMuxers.  AWSScanMuxers coordinate multi-account scans
    across a pool of threads which either call account scan code locally in the case of
    a local run or invoke a Lambda-per-account in the case of Altimeter running on AWS Lambda.

    Args:
        scan_id: unique scan identifier
        config: Config object
    """

    def __init__(self, scan_id: str, config: Config):
        self.scan_id = scan_id
        self.config = config

    def scan(
        self, account_scan_plan: AccountScanPlan
    ) -> Generator[AccountScanManifest, None, None]:
        """Scan accounts. Return a list of AccountScanManifest objects.

        Args:
            account_scan_plan: AccountScanPlan defining this scan op

        Yields:
            AccountScanManifest objects
        """
        num_total_accounts = len(account_scan_plan.account_ids)
        scanned_account_ids = set()
        unscanned_account_ids = set(account_scan_plan.account_ids)
        account_scan_plans = account_scan_plan.to_batches(
            max_accounts=self.config.concurrency.max_accounts_per_thread
        )
        num_account_batches = len(account_scan_plans)
        num_threads = min(num_account_batches, self.config.concurrency.max_account_scan_threads)
        logger = Logger()
        with logger.bind(
            num_total_accounts=num_total_accounts,
            num_account_batches=num_account_batches,
            muxer=self.__class__.__name__,
            num_muxer_threads=num_threads,
        ):
            logger.info(event=AWSLogEvents.MuxerStart)
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                processed_accounts = 0
                futures = []
                for sub_account_scan_plan in account_scan_plans:
                    account_scan_future = self._schedule_account_scan(
                        executor, sub_account_scan_plan
                    )
                    futures.append(account_scan_future)
                    logger.info(
                        event=AWSLogEvents.MuxerQueueScan,
                        account_ids=",".join(sub_account_scan_plan.account_ids),
                    )
                for future in as_completed(futures):
                    scan_results_dicts = future.result()
                    for scan_results_dict in scan_results_dicts:
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
                        yield account_scan_result
                        processed_accounts += 1
                        scanned_account_ids.add(account_id)
                        unscanned_account_ids.remove(account_id)
                    logger.info(
                        event=AWSLogEvents.MuxerStat,
                        num_scanned=processed_accounts,
                        scanned_account_ids=sorted(scanned_account_ids),
                        unscanned_account_ids=sorted(unscanned_account_ids),
                    )
            logger.info(event=AWSLogEvents.MuxerEnd)

    @abc.abstractmethod
    def _schedule_account_scan(
        self, executor: ThreadPoolExecutor, account_scan_plan: AccountScanPlan
    ) -> Future:
        """Given a ThreadPoolExecutor and scan details (date/time/accounts/regions),
        schedule an account scan by making a call to executor.submit and return the Future
        returned by executor.submit.

        In a local environment this means creating an AccountScanner directly and
        calling executor.submit(AccountScanner.scan)
        """
        raise NotImplementedError
