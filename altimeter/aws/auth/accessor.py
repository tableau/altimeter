"""An Accessor consists of a list of MultiHopAccessors.  It provides a method `get_session`
which will iterate through the MultiHopAccessors until a session can be obtained to
a target account."""
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

import boto3

from altimeter.aws.auth.cache import AWSCredentialsCache
from altimeter.aws.auth.exceptions import AccountAuthException
from altimeter.aws.auth.multi_hop_accessor import MultiHopAccessor
from altimeter.core.log import Logger
from altimeter.core.log_events import LogEvent


@dataclass(frozen=True)
class Accessor:
    """An Accessor consists of a list of MultiHopAccessors.  It provides a method `get_session`
    which will iterate through the MultiHopAccessors until a session can be obtained to
    a target account.  If an Accessor has no MultiHopAccessors it simply uses the local
    session to attempt to access the account. If the session does not match the requested
    target account id, ValueError is thrown.

    Args:
        multi_hop_accessors: List of MultiHopAccessors
        credentials_cache: AWSCredentialsCache
    """

    multi_hop_accessors: List[MultiHopAccessor]
    credentials_cache: Optional[AWSCredentialsCache] = None

    def get_session(self, account_id: str, region_name: Optional[str] = None) -> boto3.Session:
        """Get a boto3 session for a given account.

        Args:
            account_id: target account id
            region_name: session region name

        Returns:
            boto3.Session object
        """
        logger = Logger()
        errors = []
        if self.multi_hop_accessors:
            for mha in self.multi_hop_accessors:  # pylint: disable=not-an-iterable
                with logger.bind(auth_accessor=str(mha)):
                    try:
                        session = mha.get_session(
                            account_id=account_id,
                            region_name=region_name,
                            credentials_cache=self.credentials_cache,
                        )
                        return session
                    except Exception as ex:
                        errors.append(ex)
                        logger.debug(event=LogEvent.AuthToAccountFailure, exception=str(ex))
            raise AccountAuthException(f"Unable to access {account_id} using {str(self)}: {errors}")
        # local run mode
        session = boto3.Session(region_name=region_name)
        sts_client = session.client("sts")
        sts_account_id = sts_client.get_caller_identity()["Account"]
        if sts_account_id != account_id:
            raise ValueError(f"BUG: sts_account_id {sts_account_id} != {account_id}")
        return session

    def __str__(self) -> str:
        return ",".join(
            [str(mha) for mha in self.multi_hop_accessors]  # pylint: disable=not-an-iterable
        )

    @classmethod
    def from_dict(
        cls: Type["Accessor"], data: Dict[str, Any], cache_creds: bool = True
    ) -> "Accessor":
        mhas = data.get("multi_hop_accessors", [])
        credentials_cache = None
        credentials_cache_dict = data.get("credentials_cache")
        if credentials_cache_dict is None:
            if cache_creds:
                credentials_cache = AWSCredentialsCache()
        else:
            credentials_cache = AWSCredentialsCache.from_dict(credentials_cache_dict)
        return cls(
            multi_hop_accessors=[MultiHopAccessor.from_dict(mha) for mha in mhas],
            credentials_cache=credentials_cache,
        )

    @classmethod
    def from_file(cls: Type["Accessor"], filepath: Path, cache_creds: bool = True) -> "Accessor":
        """Create an Accessor from json content in a file

        Args:
            filepath: Path to json accessor definition

        Returns:
            Accessor
        """
        with filepath.open("r") as fp:
            config_dict = json.load(fp)
        if cache_creds:
            credentials_cache = AWSCredentialsCache()
            config_dict["credentials_cache"] = credentials_cache.to_dict()
        return cls.from_dict(config_dict)

    def to_dict(self) -> Dict[str, Any]:
        credentials_cache_dict = (
            None if self.credentials_cache is None else self.credentials_cache.to_dict()
        )
        return {
            "multi_hop_accessors": [mha.to_dict() for mha in self.multi_hop_accessors],
            "credentials_cache": credentials_cache_dict,
        }
