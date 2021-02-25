"""AWSScanMuxer that runs account scans one-per-lambda"""
from concurrent.futures import Future, ThreadPoolExecutor
import json
from typing import Tuple

import boto3
import botocore

from altimeter.aws.log_events import AWSLogEvents
from altimeter.aws.scan.account_scanner import AccountScanResult
from altimeter.aws.scan.muxer import AWSScanMuxer
from altimeter.aws.scan.scan_plan import AccountScanPlan
from altimeter.core.base_model import BaseImmutableModel
from altimeter.core.config import AWSConfig
from altimeter.core.log import Logger


class AccountScanLambdaEvent(BaseImmutableModel):
    account_scan_plan: AccountScanPlan
    scan_id: str
    artifact_path: str
    max_svc_scan_threads: int
    preferred_account_scan_regions: Tuple[str, ...]
    scan_sub_accounts: bool


class LambdaAWSScanMuxer(AWSScanMuxer):
    """AWSScanMuxer that runs account scans in AccountScan lambdas

    Args:
        scan_id: unique scan identifier
        account_scan_lambda_name: name of the AccountScan lambda
        account_scan_lambda_timeout: timeout for the AccountScan lambda
        config: Config object
    """

    def __init__(
        self,
        scan_id: str,
        account_scan_lambda_name: str,
        account_scan_lambda_timeout: int,
        config: AWSConfig,
    ):
        super().__init__(scan_id=scan_id, config=config)
        self.account_scan_lambda_name = account_scan_lambda_name
        self.account_scan_lambda_timeout = account_scan_lambda_timeout

    def _schedule_account_scan(
        self, executor: ThreadPoolExecutor, account_scan_plan: AccountScanPlan
    ) -> Future:
        """Schedule an account scan by calling the AccountScan lambda with
        the proper arguments."""
        lambda_event = AccountScanLambdaEvent(
            account_scan_plan=account_scan_plan,
            scan_id=self.scan_id,
            artifact_path=self.config.artifact_path,
            max_svc_scan_threads=self.config.concurrency.max_svc_scan_threads,
            preferred_account_scan_regions=self.config.scan.preferred_account_scan_regions,
            scan_sub_accounts=self.config.scan.scan_sub_accounts,
        )
        return executor.submit(
            invoke_lambda,
            self.account_scan_lambda_name,
            self.account_scan_lambda_timeout,
            lambda_event,
        )


def invoke_lambda(
    lambda_name: str, lambda_timeout: int, account_scan_lambda_event: AccountScanLambdaEvent
) -> AccountScanResult:
    """Invoke the AccountScan AWS Lambda function

    Args:
        lambda_name: name of lambda
        lambda_timeout: timeout of the lambda. Used to tell the boto3 lambda client to wait
                        at least this long for a response before timing out.
        account_scan_lambda_event: AccountScanLambdaEvent object to serialize to json and send to the lambda

    Returns:
        AccountScanResult

    Raises:
        Exception if there was an error invoking the lambda.
    """
    logger = Logger()
    account_id = account_scan_lambda_event.account_scan_plan.account_id
    with logger.bind(lambda_name=lambda_name, lambda_timeout=lambda_timeout, account_id=account_id):
        logger.info(event=AWSLogEvents.RunAccountScanLambdaStart)
        boto_config = botocore.config.Config(
            read_timeout=lambda_timeout + 10, retries={"max_attempts": 0},
        )
        session = boto3.Session()
        lambda_client = session.client("lambda", config=boto_config)
        try:
            resp = lambda_client.invoke(
                FunctionName=lambda_name, Payload=account_scan_lambda_event.json().encode("utf-8")
            )
        except Exception as invoke_ex:
            error = str(invoke_ex)
            logger.info(event=AWSLogEvents.RunAccountScanLambdaError, error=error)
            raise Exception(
                f"Error while invoking {lambda_name} with event {account_scan_lambda_event.json()}: {error}"
            ) from invoke_ex
        payload: bytes = resp["Payload"].read()
        if resp.get("FunctionError", None):
            function_error = payload.decode()
            logger.info(event=AWSLogEvents.RunAccountScanLambdaError, error=function_error)
            raise Exception(
                f"Function error in {lambda_name} with event {account_scan_lambda_event.json()}: {function_error}"
            )
        payload_dict = json.loads(payload)
        account_scan_result = AccountScanResult(**payload_dict)
        logger.info(event=AWSLogEvents.RunAccountScanLambdaEnd)
        return account_scan_result
