"""Classes for caching AWS credentials"""
import time
from typing import Dict, Optional

import boto3
from pydantic import Field

from altimeter.core.base_model import BaseImmutableModel


class AWSCredentials(BaseImmutableModel):
    """Represents a set of AWS Credentials

    Args:
        access_key_id: AWS access key id
        secret_access_key: AWS secret access key
        session_token: AWS session token
        expiration: Session expiration as an epoch timestamp int
    """

    access_key_id: str
    secret_access_key: str
    session_token: str
    expiration: int

    def is_expired(self) -> bool:
        """Determine if this cache value is within 60 seconds of expiry

        Returns:
            True if this session is value is expired, else False.
        """
        return int(time.time()) >= self.expiration - 60

    def get_session(self, region_name: Optional[str] = None) -> boto3.Session:
        """Build a boto3.Session using these credentials"""
        return boto3.Session(
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            aws_session_token=self.session_token,
            region_name=region_name,
        )


class AWSCredentialsCache(BaseImmutableModel):
    """An AWSCredentialsCache is a cache for AWSCredentials."""

    # https://github.com/PyCQA/pylint/issues/2698
    # pylint: disable=unsupported-assignment-operation,no-member,unsupported-delete-operation,unsubscriptable-object

    cache: Dict[str, AWSCredentials] = Field(default_factory=dict)

    @staticmethod
    def build_cache_key(account_id: str, role_name: str, role_session_name: str) -> str:
        return ":".join((account_id, role_name, role_session_name))

    def put(
        self, credentials: AWSCredentials, account_id: str, role_name: str, role_session_name: str
    ) -> None:
        """Put an AWSCredentials object into the cache.

        Args:
            credentials: credentials to cache
            account_id: session account id
            role_name: session role name
            role_session_name: session role session name
        """
        session = credentials.get_session()
        sts_client = session.client("sts")
        caller_id = sts_client.get_caller_identity()
        # make sure these creds are for the account the caller claims them to be
        if caller_id["Account"] != account_id:
            raise ValueError(
                (
                    f"Credentials {credentials} are not for claimed account_id "
                    f"{account_id} - they are for {caller_id['Account']}"
                )
            )
        cache_key = AWSCredentialsCache.build_cache_key(account_id, role_name, role_session_name)
        self.cache[cache_key] = credentials

    def get(
        self,
        account_id: str,
        role_name: str,
        role_session_name: str,
        region_name: Optional[str] = None,
    ) -> Optional[boto3.Session]:
        """Get a boto3 Session from AWSCredentials in the cache. Return None if no matching
        AWSCredentials were found.

        Args:
            account_id: session account id
            role_name: session role name
            role_session_name: session role session name
            region_name: session region_name

        Returns:
            boto3.Session from credentials if cached, else None.
        """
        cache_key = AWSCredentialsCache.build_cache_key(account_id, role_name, role_session_name)
        cache_val = self.cache.get(cache_key)
        if cache_val is not None:
            if cache_val.is_expired():
                del self.cache[cache_key]
            else:
                return self.cache[cache_key].get_session(region_name=region_name)
        return None
