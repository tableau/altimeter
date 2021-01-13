"""An Accessor consists of a list of MultiHopAccessors.  It provides a method `get_session`
which will iterate through the MultiHopAccessors until a session can be obtained to
a target account."""
import json
from pathlib import Path
from typing import List, Optional, Type

import boto3
from pydantic import Field

from altimeter.aws.auth.cache import AWSCredentialsCache
from altimeter.aws.auth.exceptions import AccountAuthException
from altimeter.aws.auth.multi_hop_accessor import MultiHopAccessor
from altimeter.core.log import Logger
from altimeter.core.log_events import LogEvent
from altimeter.core.base_model import BaseImmutableModel


class Accessor(BaseImmutableModel):
    """An Accessor consists of a list of MultiHopAccessors.  It provides a method `get_session`
    which will iterate through the MultiHopAccessors until a session can be obtained to
    a target account.  If an Accessor has no MultiHopAccessors it simply uses the local
    session to attempt to access the account. If the session does not match the requested
    target account id, ValueError is thrown.

    Args:
        multi_hop_accessors: List of MultiHopAccessors
        credentials_cache: AWSCredentialsCache
    """

    credentials_cache: AWSCredentialsCache = Field(default_factory=AWSCredentialsCache)
    multi_hop_accessors: List[MultiHopAccessor] = Field(default_factory=list)
    cache_creds: bool = True

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
                        if self.cache_creds:
                            session = mha.get_session(
                                account_id=account_id,
                                region_name=region_name,
                                credentials_cache=self.credentials_cache,
                            )
                        else:
                            session = mha.get_session(
                                account_id=account_id, region_name=region_name,
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
    def from_file(cls: Type["Accessor"], filepath: Path, cache_creds: bool = True) -> "Accessor":
        """Create an Accessor from json content in a file

        Args:
            filepath: Path to json accessor definition

        Returns:
            Accessor
        """
        with filepath.open("r") as fp:
            config_dict = json.load(fp)
        return cls(**config_dict, cache_creds=cache_creds)
