"""Base remediator class"""
from typing import Any, Dict, List, Optional

import boto3
from pydantic import BaseSettings

from altimeter.core.log import Logger
from altimeter.qj.exceptions import RemediationError
from altimeter.qj.log import QJLogEvents
from altimeter.qj.schemas.result_set import Result


class Config(BaseSettings):
    dry_run: bool
    remediator_target_role_name: str
    remediator_target_role_external_id: Optional[str] = None


class RemediatorLambda:
    @classmethod
    def remediate(cls, session: boto3.Session, result: Dict[str, Any], dry_run: bool) -> None:
        raise NotImplementedError("RemediatorLambda.remediate must be implemented in subclasses")

    @classmethod
    def lambda_handler(cls, event: Dict[str, Any], _: Any) -> None:
        """lambda entrypoint"""
        config = Config()
        result = Result(**event)
        logger = Logger()
        errors: List[str] = []
        with logger.bind(result=result):
            logger.info(event=QJLogEvents.ResultRemediationStart)
            try:
                session = get_assumed_session(
                    account_id=result.account_id,
                    role_name=config.remediator_target_role_name,
                    external_id=config.remediator_target_role_external_id,
                )
                cls.remediate(session=session, result=result.result, dry_run=config.dry_run)
                logger.info(event=QJLogEvents.ResultRemediationSuccessful)
            except Exception as ex:
                logger.error(event=QJLogEvents.ResultRemediationFailed, error=str(ex))
                errors.append(str(ex))
        if errors:
            raise RemediationError(f"Errors found during remediation: {errors}")


def get_assumed_session(
    account_id: str, role_name: str, external_id: Optional[str] = None
) -> boto3.Session:
    cws = boto3.Session()
    sts_client = cws.client("sts")
    role_arn = f"arn:aws:iam::{account_id}:role/{role_name}"
    assume_args = {"RoleArn": role_arn, "RoleSessionName": "qj-remediator"}
    if external_id is not None:
        assume_args["ExternalId"] = external_id
    assume_resp = sts_client.assume_role(**assume_args)
    creds = assume_resp["Credentials"]
    return boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
    )
