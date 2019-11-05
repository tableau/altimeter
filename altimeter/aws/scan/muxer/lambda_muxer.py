"""AWSScanMuxer that runs account scans one-per-lambda"""
from concurrent.futures import Future, ThreadPoolExecutor
import json
from typing import Any, Dict

import boto3
import botocore

from altimeter.core.log import Logger
from altimeter.aws.log import AWSLogEvents
from altimeter.aws.scan.muxer import AWSScanMuxer
from altimeter.aws.scan.account_scan_plan import AccountScanPlan


class LambdaAWSScanMuxer(AWSScanMuxer):
    """AWSScanMuxer that runs account scans one-per-lambda

    Args:
        account_scan_lambda_name: name of the AccountScan lambda
        account_scan_lambda_timeout: timeout for the AccountScan lambda
        json_bucket: bucket to dump json output into
        key_prefix: prefix for json output objects
        scan_sub_accounts: if True, scan subaccounts of any org master accounts
        max_lambdas: max number of AccountScan lambdas to run concurrently
    """

    def __init__(
        self,
        account_scan_lambda_name: str,
        account_scan_lambda_timeout: int,
        json_bucket: str,
        key_prefix: str,
        scan_sub_accounts: bool,
        max_lambdas: int,
    ):
        super().__init__(max_threads=max_lambdas)
        self.account_scan_lambda_name = account_scan_lambda_name
        self.account_scan_lambda_timeout = account_scan_lambda_timeout
        self.json_bucket = json_bucket
        self.key_prefix = key_prefix
        self.scan_sub_accounts = scan_sub_accounts

    def _schedule_account_scan(
        self, executor: ThreadPoolExecutor, account_scan_plan: AccountScanPlan
    ) -> Future:
        """Schedule an account scan by calling the AccountScan lambda with
        the proper arguments."""
        lambda_event = {
            "account_scan_plan": account_scan_plan.to_dict(),
            "regions": account_scan_plan.regions,
            "json_bucket": self.json_bucket,
            "key_prefix": self.key_prefix,
            "scan_sub_accounts": self.scan_sub_accounts,
        }
        return executor.submit(
            invoke_lambda,
            self.account_scan_lambda_name,
            self.account_scan_lambda_timeout,
            lambda_event,
        )


def invoke_lambda(lambda_name: str, lambda_timeout: int, event: Dict[str, Any]) -> Dict[str, Any]:
    """Invoke an AWS Lambda function

    Args:
        lambda_name: name of lambda
        lambda_timeout: timeout of the lambda. Used to tell the boto3 lambda client to wait
                        at least this long for a response before timing out.
        event: event data to send to the lambda

    Returns:
        lambda response payload

    Raises:
        Exception if there was an error invoking the lambda.
    """
    logger = Logger()
    with logger.bind(lambda_name=lambda_name, lambda_timeout=lambda_timeout, event=event):
        logger.info(event=AWSLogEvents.RunAccountScanLambdaStart)
        boto_config = botocore.config.Config(
            read_timeout=lambda_timeout + 10, retries={"max_attempts": 0}
        )
        session = boto3.Session()
        lambda_client = session.client("lambda", config=boto_config)
        resp = lambda_client.invoke(
            FunctionName=lambda_name, Payload=json.dumps(event).encode("utf-8")
        )
        payload: bytes = resp["Payload"].read()
        if resp.get("FunctionError", None):
            raise Exception(f"Error invoking {lambda_name} with event {event}: {payload}")
        payload_dict = json.loads(payload)
        logger.info(event=AWSLogEvents.RunAccountScanLambdaEnd)
        return payload_dict
