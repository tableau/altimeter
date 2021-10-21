"""Remediate results of a QJ"""
import json
from typing import Any, Dict

from altimeter.core.log import Logger
from altimeter.qj.client import QJAPIClient
from altimeter.qj.config import RemediatorConfig
from altimeter.qj.log import QJLogEvents
from altimeter.qj.schemas.remediation import Remediation


class RemediationError(Exception):
    """An error during Remediation"""


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
        for result in latest_result_set.results:
            logger.info(event=QJLogEvents.ProcessResult, result=result)
