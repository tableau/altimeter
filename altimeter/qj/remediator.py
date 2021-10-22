"""Base remediator class"""
from typing import Any, Dict, List

from altimeter.core.log import Logger
from altimeter.qj.exceptions import RemediationError
from altimeter.qj.log import QJLogEvents
from altimeter.qj.schemas.result_set import Result


class RemediatorLambda:
    @classmethod
    def remediate(cls, result: Result) -> None:
        raise NotImplementedError("RemediatorLambda.remediate must be implemented in subclasses")

    @classmethod
    def lambda_handler(cls, event: Dict[str, Any], _: Any) -> None:
        """lambda entrypoint"""
        result = Result(**event)
        logger = Logger()
        errors: List[str] = []
        with logger.bind(result=result):
            logger.info(event=QJLogEvents.ResultRemediationStart)
            try:
                cls.remediate(result)
                logger.info(event=QJLogEvents.ResultRemediationSuccessful)
            except Exception as ex:
                logger.error(event=QJLogEvents.ResultRemediationFailed, error=str(ex))
                errors.append(str(ex))
        if errors:
            raise RemediationError(f"Errors found during remediation: {errors}")
