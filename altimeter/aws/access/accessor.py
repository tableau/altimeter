"""Classes defining multi-stage AWS access methods. Provides the ability to
auth to an account via 1+ bridge accounts and to try multiple methods."""
from dataclasses import dataclass, field
from datetime import datetime
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

import boto3

from altimeter.aws.access.exceptions import AccountAuthException
from altimeter.core.log import Logger
from altimeter.core.log_events import LogEvent


@dataclass(frozen=True)
class SessionCacheValue:
    """A SessionCacheValue represents a value in a :class:`.SessionCache`.

    Args:
        session: boto3 Session object
        expiration: expiration time for this cache value.
    """

    session: boto3.Session
    expiration: datetime

    def is_expired(self) -> bool:
        """Determine if this cache value is expired

        Returns:
            True if this session is value is expired, else False.
        """
        return datetime.utcnow().replace(tzinfo=None) >= self.expiration.replace(tzinfo=None)


class SessionCache:
    """A SessionCache is a cache for boto3 Sessions."""

    def __init__(self) -> None:
        self._cache: Dict[str, SessionCacheValue] = {}

    @staticmethod
    def _build_key(
        account_id: str, role_name: str, role_session_name: str, region: Optional[str]
    ) -> str:
        """Build a key for a :class:`.SessionCache` representing a unique Session.

        Args:
            account_id: session account id
            role_name: session role name
            role_session_name: session role session name
            region: session region

        Returns:
            string cache key
        """
        return f"{account_id}:{role_name}:{role_session_name}:{region}"

    def put(
        self,
        session: boto3.Session,
        expiration: datetime,
        account_id: str,
        role_name: str,
        role_session_name: str,
        region: Optional[str] = None,
    ) -> None:
        """Put a Session into the cache.

        Args:
            session: session to cache
            expiration: expiration time for this entry
            account_id: session account id
            role_name: session role name
            role_session_name: session role session name
            region: session region
        """
        cache_key = SessionCache._build_key(account_id, role_name, role_session_name, region)
        self._cache[cache_key] = SessionCacheValue(session, expiration)

    def get(
        self, account_id: str, role_name: str, role_session_name: str, region: Optional[str] = None
    ) -> Optional[SessionCacheValue]:
        """Get a session from the cache.

        Args:
            account_id: session account id
            role_name: session role name
            role_session_name: session role session name
            region: session region

        Returns:
            SessionCacheValue if one is found, else None.
        """
        cache_key = SessionCache._build_key(account_id, role_name, role_session_name, region)
        cache_val = self._cache.get(cache_key)
        if cache_val is not None:
            if cache_val.is_expired():
                del self._cache[cache_key]
            else:
                return self._cache[cache_key]
        return None


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

    def to_dict(self) -> Dict[str, Any]:
        """Generate a dict representation of this AccessStep

        Returns:
            dict representation of this AccessStep
        """
        return {
            "role_name": self.role_name,
            "external_id": self.external_id,
            "account_id": self.account_id,
        }

    @classmethod
    def from_dict(cls: Type["AccessStep"], data: Dict[str, Any]) -> "AccessStep":
        """Create an AccessStep from a dict containing AccessStep data.
        Args:
            data: AccessStep data dict

        Returns:
            AccessStep object

        Raises:
            ValueError if data is not valid
        """
        role_name = data.get("role_name")
        if role_name is None:
            raise ValueError(f"{cls.__name__} missing key 'role_name': {data}")
        external_id = data.get("external_id")
        if not external_id:
            external_id_env_var = data.get("external_id_env_var")
            if external_id_env_var is not None:
                external_id = os.environ.get(external_id_env_var)
                if external_id is None:
                    raise ValueError(
                        f"Missing env var '{external_id_env_var}' for {cls.__name__} {data}"
                    )
        account_id = data.get("account_id")
        return cls(role_name=role_name, external_id=external_id, account_id=account_id)


class MultiHopAccessor:
    """A MultiHopAccessor contains a list of AccessSteps defining how to gain access to an account.

    Args:
        role_session_name: role session name to use for session creation.
        access_steps: list of AccessSteps defining how to access a final
                      destination account.
    """

    def __init__(self, role_session_name: str, access_steps: List[AccessStep]):
        self.role_session_name = role_session_name
        if not access_steps:
            raise ValueError("One or more access steps must be specified")
        for access_step in access_steps[:-1]:
            if not access_step.account_id:
                raise ValueError(
                    "Non-final AccessStep of a MultiHopAccessor must specify an account_id"
                )
        if access_steps[-1].account_id:
            raise ValueError(
                "The last AccessStep of a MultiHopAccessor must not specify account_id"
            )
        self.access_steps = access_steps
        self.session_cache = SessionCache()

    def get_session(self, account_id: str, region: Optional[str] = None) -> boto3.Session:
        """Get a session for an account_id by iterating through the :class:`.AccessStep`s
        of this :class:`.MultiHopAccessor`.

        Args:
             account_id: account to access
             region: region to use during session creation.

        Returns:
            boto3 Session for accessing account_id
        """
        logger = Logger()
        cws = boto3.Session(region_name=region)
        for access_step in self.access_steps:
            access_account_id = access_step.account_id if access_step.account_id else account_id
            role_name = access_step.role_name
            external_id = access_step.external_id
            session_cache_value = self.session_cache.get(
                account_id=access_account_id,
                role_name=role_name,
                role_session_name=self.role_session_name,
                region=region,
            )
            if session_cache_value is None:
                logger.debug(event=LogEvent.AuthToAccountStart)
                sts_client = cws.client("sts")
                role_arn = f"arn:aws:iam::{access_account_id}:role/{role_name}"
                assume_args = {"RoleArn": role_arn, "RoleSessionName": self.role_session_name}
                if external_id:
                    assume_args["ExternalId"] = external_id

                assume_resp = sts_client.assume_role(**assume_args)
                creds = assume_resp["Credentials"]
                expiration = creds["Expiration"]
                cws = boto3.Session(
                    aws_access_key_id=creds["AccessKeyId"],
                    aws_secret_access_key=creds["SecretAccessKey"],
                    aws_session_token=creds["SessionToken"],
                    region_name=region,
                )
                self.session_cache.put(
                    session=cws,
                    expiration=expiration,
                    account_id=access_account_id,
                    role_name=role_name,
                    role_session_name=self.role_session_name,
                    region=region,
                )
                logger.debug(event=LogEvent.AuthToAccountEnd)
            else:
                cws = session_cache_value.session
        return cws

    def __str__(self) -> str:
        return f'accessor:{self.role_session_name}:{",".join([str(access_step) for access_step in self.access_steps])}'

    def to_dict(self) -> Dict[str, Any]:
        """Generate a dict representation of this MultiHopAccessor

        Returns:
            dict representation of this MultiHopAccessor
        """
        return {
            "role_session_name": self.role_session_name,
            "access_steps": [access_step.to_dict() for access_step in self.access_steps],
        }

    @classmethod
    def from_dict(cls: Type["MultiHopAccessor"], data: Dict[str, Any]) -> "MultiHopAccessor":
        """Build a MultiHopAccessor from a dict representation.

        Args:
            data: dict of data representing a MultiHopAccessor
        Returns:
            MultiHopAccessor object
        """
        access_step_dicts = data.get("access_steps")
        if access_step_dicts is None:
            raise ValueError(f"{cls.__name__} missing key 'access_steps': {data}")
        access_steps = [
            AccessStep.from_dict(access_step_dict) for access_step_dict in access_step_dicts
        ]
        role_session_name = data.get("role_session_name")
        if role_session_name is None:
            raise ValueError(f"{cls.__name__} missing key 'role_session_name': {data}")
        return cls(role_session_name, access_steps)


@dataclass(frozen=True)
class Accessor:
    """An Accessor consists of a list of MultiHopAccessors.  It provides a method `get_session`
    which will iterate through the MultiHopAccessors until a session can be obtained to
    a target account.  If an Accessor has no MultiHopAccessors it simply uses the local
    session to attempt to access the account. If the session does not match the requested
    target account id, ValueError is thrown.

    Args:
        multi_hop_accessors: List of MultiHopAccessors
    """

    multi_hop_accessors: List[MultiHopAccessor] = field(default_factory=list)

    def get_session(self, account_id: str, region: Optional[str] = None) -> boto3.Session:
        """Get a boto3 session for a given account.

        Args:
            account_id: target account id
            region: session region

        Returns:
            boto3.Session object
        """
        logger = Logger()
        with logger.bind(auth_account_id=account_id):
            if self.multi_hop_accessors:
                for mha in self.multi_hop_accessors:  # pylint: disable=not-an-iterable
                    with logger.bind(auth_accessor=str(mha)):
                        try:
                            session = mha.get_session(account_id=account_id, region=region)
                            return session
                        except Exception as ex:
                            logger.debug(event=LogEvent.AuthToAccountFailure, exception=str(ex))

                raise AccountAuthException(f"Unable to access {account_id} using {str(self)}")
            # local run mode
            session = boto3.Session(region_name=region)
            sts_client = session.client("sts")
            sts_account_id = sts_client.get_caller_identity()["Account"]
            if sts_account_id != account_id:
                raise ValueError(f"BUG: sts_account_id {sts_account_id} != {account_id}")
            return session

    def __str__(self) -> str:
        return ", ".join(
            [str(mha) for mha in self.multi_hop_accessors]  # pylint: disable=not-an-iterable
        )

    def to_dict(self) -> Dict[str, Any]:
        """Generate a dict representation of this Accessor.

        Returns:
            dict representation of this Accessor.
        """
        mha_dicts = [
            mha.to_dict() for mha in self.multi_hop_accessors  # pylint: disable=not-an-iterable
        ]
        data = {"accessors": mha_dicts}
        return data

    @classmethod
    def from_dict(cls: Type["Accessor"], data: Dict[str, Any]) -> "Accessor":
        """Create an Accessor from a dict representation.

        Args:
            data: dict representation of an Accessor

        Returns:
            Accessor object
        """
        multi_hop_accessors: List[MultiHopAccessor] = []
        accessor_dicts = data.get("accessors", [])
        for accessor_dict in accessor_dicts:
            multi_hop_accessor = MultiHopAccessor.from_dict(accessor_dict)
            multi_hop_accessors.append(multi_hop_accessor)
        return cls(multi_hop_accessors=multi_hop_accessors)

    @classmethod
    def from_file(cls: Type["Accessor"], filepath: Path) -> "Accessor":
        """Create an Accessor from json content in a file

        Args:
            filepath: Path to json accessor definition

        Returns:
            Accessor
        """
        with filepath.open("r") as fp:
            data = json.load(fp)
        return cls.from_dict(data)
