"""A MultiHopAccessor contains a list of AccessSteps defining how to gain access to an account via
role assumption(s)."""
from dataclasses import asdict, dataclass, field
import os
from typing import Any, Dict, List, Optional, Type

import boto3
import jinja2

from altimeter.aws.auth.cache import AWSCredentials, AWSCredentialsCache
from altimeter.core.log import Logger
from altimeter.core.log_events import LogEvent


@dataclass(frozen=True)
class AccessStep:
    """Represents a single access step to get to an account.

    Args:
        role_name: role name for this step
        account_id: account_id for this step. If empty this step is assumed to be the last
                    in a chain of multiple AccessSteps
        external_id: external_id to use for access (if needed).
    """

    role_name: str
    account_id: Optional[str] = field(default=None)
    external_id: Optional[str] = field(default=None)

    def __str__(self) -> str:
        account = self.account_id if self.account_id else "target"
        return f"{self.role_name}@{account}"

    @classmethod
    def from_dict(cls: Type["AccessStep"], data: Dict[str, Any]) -> "AccessStep":
        role_name = data.get("role_name")
        if role_name is None:
            raise ValueError(f"AccessStep '{data}' missing key 'role_name'")
        account_id = data.get("account_id")
        external_id = data.get("external_id")
        if external_id is not None:
            template = jinja2.Environment(
                loader=jinja2.BaseLoader(), undefined=jinja2.StrictUndefined
            ).from_string(external_id)
            external_id = template.render(env=os.environ)
        return cls(role_name=role_name, account_id=account_id, external_id=external_id)


@dataclass(frozen=True)
class MultiHopAccessor:
    """A MultiHopAccessor contains a list of AccessSteps defining how to gain access to an account
    via role assumption(s).

    Args:
        role_session_name: role session name to use for session creation.
        access_steps: list of AccessSteps defining how to access a final
                      destination account.
    """

    role_session_name: str
    access_steps: List[AccessStep]

    def __post_init__(self) -> None:
        if not self.access_steps:
            raise ValueError("One or more access steps must be specified")
        for access_step in self.access_steps[:-1]:
            if not access_step.account_id:
                raise ValueError(
                    "Non-final AccessStep of a MultiHopAccessor must specify an account_id"
                )
        if self.access_steps[-1].account_id:
            raise ValueError(
                "The last AccessStep of a MultiHopAccessor must not specify account_id"
            )

    def get_session(
        self,
        account_id: str,
        region_name: Optional[str] = None,
        credentials_cache: Optional[AWSCredentialsCache] = None,
    ) -> boto3.Session:
        """Get a session for an account_id by iterating through the :class:`.AccessStep`s
        of this :class:`.MultiHopAccessor`.

        Args:
             account_id: account to access
             region_name: region to use during session creation.

        Returns:
            boto3 Session for accessing account_id
        """
        logger = Logger()
        cws = boto3.Session(region_name=region_name)
        for access_step in self.access_steps:
            access_account_id = access_step.account_id if access_step.account_id else account_id
            role_name = access_step.role_name
            external_id = access_step.external_id
            session = None
            if credentials_cache is not None:
                session = credentials_cache.get(
                    account_id=access_account_id,
                    role_name=role_name,
                    role_session_name=self.role_session_name,
                    region_name=region_name,
                )
            if session is None:
                logger.debug(event=LogEvent.AuthToAccountStart)
                sts_client = cws.client("sts")
                role_arn = f"arn:aws:iam::{access_account_id}:role/{role_name}"
                assume_args = {"RoleArn": role_arn, "RoleSessionName": self.role_session_name}
                if external_id:
                    assume_args["ExternalId"] = external_id

                assume_resp = sts_client.assume_role(**assume_args)
                creds = assume_resp["Credentials"]
                expiration_datetime = creds["Expiration"]
                credentials = AWSCredentials(
                    access_key_id=creds["AccessKeyId"],
                    secret_access_key=creds["SecretAccessKey"],
                    session_token=creds["SessionToken"],
                    expiration=int(expiration_datetime.timestamp()),
                )
                session = boto3.Session(
                    aws_access_key_id=creds["AccessKeyId"],
                    aws_secret_access_key=creds["SecretAccessKey"],
                    aws_session_token=creds["SessionToken"],
                    region_name=region_name,
                )
                if credentials_cache is not None:
                    credentials_cache.put(
                        credentials=credentials,
                        account_id=access_account_id,
                        role_name=role_name,
                        role_session_name=self.role_session_name,
                    )
                logger.debug(event=LogEvent.AuthToAccountEnd)
            cws = session
        return session

    def __str__(self) -> str:
        return f'accessor:{self.role_session_name}:{",".join([str(access_step) for access_step in self.access_steps])}'

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls: Type["MultiHopAccessor"], data: Dict[str, Any]) -> "MultiHopAccessor":
        role_session_name = data.get("role_session_name")
        if not role_session_name:
            raise ValueError(f"Expected key 'role_session_name' in {data} with non-empty value")
        access_step_dicts = data.get("access_steps", [])
        access_steps = [
            AccessStep.from_dict(access_step_dict) for access_step_dict in access_step_dicts
        ]
        return cls(role_session_name=role_session_name, access_steps=access_steps,)
