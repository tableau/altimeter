"""Abstract base class for AWSScanMuxers."""
import abc
from concurrent.futures import as_completed, Future, ThreadPoolExecutor
import time
from typing import Generator, Tuple

from altimeter.aws.log_events import AWSLogEvents
from altimeter.aws.scan.scan_plan import AccountScanPlan, ScanPlan
from altimeter.aws.scan.account_scan_manifest import AccountScanManifest
from altimeter.core.config import AWSConfig
from altimeter.core.log import Logger


class AWSScanMuxer(abc.ABC):
    """Abstract base class for AWSScanMuxers.  AWSScanMuxers coordinate multi-account scans
    across a pool of threads which either call account scan code locally in the case of
    a local run or invoke a Lambda-per-account in the case of Altimeter running on AWS Lambda.

    Args:
        scan_id: unique scan identifier
        config: Config object
    """

    def __init__(self, scan_id: str, config: AWSConfig):
        self.scan_id = scan_id
        self.config = config

    def scan(self, scan_plan: ScanPlan) -> Generator[AccountScanManifest, None, None]:
        """Scan accounts. Return a list of AccountScanManifest objects.

        Args:
            account_scan_plan: AccountScanPlan defining this scan op

        Yields:
            AccountScanManifest objects
        """
        num_total_accounts = len(scan_plan.account_ids)
        scanned_account_ids = set()
        unscanned_account_ids = set(scan_plan.account_ids)
        num_threads = self.config.concurrency.max_account_scan_threads
        logger = Logger()
        with logger.bind(
            num_total_accounts=num_total_accounts,
            muxer=self.__class__.__name__,
            num_muxer_threads=num_threads,
        ):
            logger.info(event=AWSLogEvents.MuxerStart)
            account_id_blacklist: Tuple[str, ...] = tuple()
            for account_scan_try in range(self.config.concurrency.max_account_scan_tries):
                with logger.bind(account_scan_try=account_scan_try):
                    account_scan_plans = scan_plan.build_account_scan_plans(
                        account_id_blacklist=account_id_blacklist
                    )
                    with ThreadPoolExecutor(max_workers=num_threads) as executor:
                        processed_accounts = 0
                        futures = []
                        for account_scan_plan in account_scan_plans:
                            account_scan_future = self._schedule_account_scan(
                                executor, account_scan_plan
                            )
                            futures.append(account_scan_future)
                            logger.info(
                                event=AWSLogEvents.MuxerQueueScan,
                                account_id=account_scan_plan.account_id,
                            )
                        for future in as_completed(futures):
                            try:
                                account_scan_result = future.result()
                            except Exception as ex:
                                logger.info(
                                    event=AWSLogEvents.ScanAWSAccountError,
                                    ex=str(ex),
                                    attempt=account_scan_try + 1,
                                )
                                continue
                            account_scan_manifest = AccountScanManifest(
                                account_id=account_scan_result.account_id,
                                artifacts=account_scan_result.artifacts,
                                errors=account_scan_result.errors,
                            )
                            yield account_scan_manifest
                            processed_accounts += 1
                            scanned_account_ids.add(account_scan_result.account_id)
                            unscanned_account_ids.remove(account_scan_result.account_id)
                            logger.info(
                                event=AWSLogEvents.MuxerStat,
                                num_scanned=processed_accounts,
                                scanned_account_ids=sorted(scanned_account_ids),
                                unscanned_account_ids=sorted(unscanned_account_ids),
                            )
                if unscanned_account_ids:
                    account_id_blacklist = tuple(scanned_account_ids)
                    time.sleep(5)
                else:
                    break
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
