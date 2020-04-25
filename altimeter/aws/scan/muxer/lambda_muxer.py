"""AWSScanMuxer that runs account scans one-per-lambda"""
from concurrent.futures import Future, ThreadPoolExecutor
import json
from typing import Any, Dict

import boto3
import botocore

from altimeter.aws.log_events import AWSLogEvents
from altimeter.aws.scan.muxer import AWSScanMuxer
from altimeter.aws.scan.account_scan_plan import AccountScanPlan
from altimeter.core.config import Config
from altimeter.core.log import Logger


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
        config: Config,
    ):
        super().__init__(scan_id=scan_id, config=config)
        self.account_scan_lambda_name = account_scan_lambda_name
        self.account_scan_lambda_timeout = account_scan_lambda_timeout

    def _schedule_account_scan(
        self, executor: ThreadPoolExecutor, account_scan_plan: AccountScanPlan
    ) -> Future:
        """Schedule an account scan by calling the AccountScan lambda with
        the proper arguments."""
        lambda_event = {
            "account_scan_plan": account_scan_plan.to_dict(),
            "scan_id": self.scan_id,
            "artifact_path": self.config.artifact_path,
            "max_svc_scan_threads": self.config.concurrency.max_svc_scan_threads,
            "preferred_account_scan_regions": self.config.scan.preferred_account_scan_regions,
            "scan_sub_accounts": self.config.scan.scan_sub_accounts,
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
    with logger.bind(lambda_name=lambda_name, lambda_timeout=lambda_timeout):
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
            raise Exception(f"Error invoking {lambda_name} with event {event}: {payload.decode()}")
        payload_dict = json.loads(payload)
        logger.info(event=AWSLogEvents.RunAccountScanLambdaEnd)
        return payload_dict
