"""Remediate results of a QJ"""
from concurrent.futures import as_completed, Future, ThreadPoolExecutor
import json
from typing import Any, Dict

import boto3
import botocore

from altimeter.core.log import Logger
from altimeter.qj.client import QJAPIClient
from altimeter.qj.config import RemediatorConfig
from altimeter.qj.exceptions import RemediationError
from altimeter.qj.log import QJLogEvents
from altimeter.qj.schemas.remediation import Remediation
from altimeter.qj.schemas.result_set import Result, ResultSet


def remediator(event: Dict[str, Any]) -> None:
    """Run the remediation lambda for a QJ result set"""
    config = RemediatorConfig()
    logger = Logger()
    remediation = Remediation(**event)
    with logger.bind(remediation=remediation):
        logger.info(event=QJLogEvents.RemediationInit)
        qj_api_client = QJAPIClient(host=config.qj_api_host)
        latest_result_set = qj_api_client.get_job_latest_result_set(job_name=remediation.job_name)
        if not latest_result_set:
            msg = f"No latest_result_set present for {remediation.job_name}"
            logger.error(QJLogEvents.StaleResultSet, detail=msg)
            raise RemediationError(msg)
        if latest_result_set.result_set_id != remediation.result_set_id:
            msg = (
                f"Remediation result_set_id {remediation.result_set_id} does not match the "
                f"latest result_set_id {latest_result_set.result_set_id}"
            )
            logger.error(QJLogEvents.StaleResultSet, detail=msg)
            raise RemediationError(msg)
        if not latest_result_set.job.remediate_sqs_queue:
            msg = f"Job {latest_result_set.job.name} has no remediator"
            logger.error(QJLogEvents.JobHasNoRemediator, detail=msg)
            raise RemediationError(msg)
        if latest_result_set.job.remediate_sqs_queue.startswith("arn:aws:sqs:"):
            remediate_via_sqs(
                remediate_sqs_queue=latest_result_set.job.remediate_sqs_queue,
                latest_result_set=latest_result_set,
                logger=logger,
            )
        else:
            remediate_via_lambda(
                lambda_name=latest_result_set.job.remediate_sqs_queue,
                latest_result_set=latest_result_set,
                logger=logger,
            )


def remediate_via_sqs(
    remediate_sqs_queue: str, latest_result_set: ResultSet, logger: Logger
) -> None:
    try:
        remediate_sqs_queue_region_name = remediate_sqs_queue.split(":")[3]
        remediate_sqs_queue_name = remediate_sqs_queue.split(":")[5]
    except IndexError as i_e:
        err_msg = (
            f"{latest_result_set.job.name} field remediate_sqs_queue "
            f"{remediate_sqs_queue} is invalid"
        )
        logger.error(QJLogEvents.InvalidRemediatorSQSQueueArn, detail=err_msg)
        raise RemediationError(err_msg) from i_e
    sqs_client = boto3.client("sqs", region_name=remediate_sqs_queue_region_name)
    try:
        remediate_sqs_queue_url = sqs_client.get_queue_url(QueueName=remediate_sqs_queue_name)[
            "QueueUrl"
        ]
    except Exception as ex:
        err_msg = f"Error getting SQS queue url for {remediate_sqs_queue_name}"
        logger.error(
            QJLogEvents.UnknownRemediatorSQSQueue,
            queue_name=remediate_sqs_queue_name,
            queue_region=remediate_sqs_queue_region_name,
            detail=err_msg,
        )
        raise RemediationError(err_msg) from ex
    for result in latest_result_set.results:
        logger.info(event=QJLogEvents.SubmittingResultRemediation, result=result)
        try:
            sqs_client.send_message(
                QueueUrl=remediate_sqs_queue_url, MessageBody=result.dict(),
            )
            logger.info(event=QJLogEvents.SubmittedResultRemediation, result=result)
        except Exception as ex:
            logger.error(
                event=QJLogEvents.FailedSubmittingResultRemediation, result=result, detail=ex,
            )


def remediate_via_lambda(lambda_name: str, latest_result_set: ResultSet, logger: Logger) -> None:
    num_threads = 10  # TODO env var
    errors = []
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for result in latest_result_set.results:
            logger.info(event=QJLogEvents.ProcessResult, result=result)
            future = _schedule_result_remediation(
                executor=executor,
                lambda_name=lambda_name,
                lambda_timeout=300,  # TODO env var?
                result=result,
            )
            futures.append(future)
        for future in as_completed(futures):
            try:
                lambda_result = future.result()
                logger.info(QJLogEvents.ResultRemediationSuccessful, lambda_result=lambda_result)
            except Exception as ex:
                logger.info(
                    event=QJLogEvents.ResultSetRemediationFailed, error=str(ex),
                )
                errors.append(str(ex))
    if errors:
        logger.error(event=QJLogEvents.ResultSetRemediationFailed, errors=errors)
        raise RemediationError(
            f"Errors encountered during remediation of {latest_result_set.job.name} "
            f"/ {latest_result_set.result_set_id}: {errors}"
        )


def _schedule_result_remediation(
    executor: ThreadPoolExecutor, lambda_name: str, lambda_timeout: int, result: Result,
) -> Future:
    """Schedule a result remediation"""
    return executor.submit(_invoke_lambda, lambda_name, lambda_timeout, result,)


def _invoke_lambda(lambda_name: str, lambda_timeout: int, result: Result,) -> Any:
    """Invoke a QJ's remediator function"""
    logger = Logger()
    with logger.bind(lambda_name=lambda_name, lambda_timeout=lambda_timeout, result=result):
        logger.info(event=QJLogEvents.InvokeResultRemediationLambdaStart)
        boto_config = botocore.config.Config(
            read_timeout=lambda_timeout + 10, retries={"max_attempts": 0},
        )
        session = boto3.Session()
        lambda_client = session.client("lambda", config=boto_config)
        event = result.json().encode("utf-8")
        try:
            resp = lambda_client.invoke(FunctionName=lambda_name, Payload=event,)
        except Exception as invoke_ex:
            error = str(invoke_ex)
            logger.info(event=QJLogEvents.InvokeResultRemediationLambdaError, error=error)
            raise Exception(
                f"Error while invoking {lambda_name} with event: {str(event)}: {error}"
            ) from invoke_ex
        lambda_result: bytes = resp["Payload"].read()
        if resp.get("FunctionError", None):
            error = lambda_result.decode()
            logger.info(event=QJLogEvents.ResultRemediationLambdaRunError, error=error)
            raise Exception(f"Function error in {lambda_name} with event {str(event)}: {error}")
        logger.info(event=QJLogEvents.InvokeResultRemediationLambdaEnd)
        return json.loads(lambda_result)
