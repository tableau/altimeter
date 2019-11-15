"""AWSAccessor is a wrapper around a boto3 client which provides protection against
non-Get/List/Describe API calls occurring as well as api call statistic tracking."""
import re
from typing import Any, Dict

from botocore.client import BaseClient
import boto3

from altimeter.core.multilevel_counter import MultilevelCounter

_PERMITTED_OPERATION_NAMES_STR = "^(Get|List|Describe).*"
_PERMITTED_OPERATION_NAMES_RE = re.compile(_PERMITTED_OPERATION_NAMES_STR)


def on_request_created(
    api_call_stats: MultilevelCounter,
    account_id: str,
    region_name: str,
    service_name: str,
    readonly: bool,
    **kwargs: Any,
) -> None:
    """Called when a boto3 request is created. This handles api call statistics tracking.

    Args:
        api_call_stats: MultilevelCounter to increment
        account_id: request account id
        region_name: request region
        service_name: request service
        readonly: if True only allow readonly calls
        kwargs: kwargs which are passed through by the boto event callback.
    """
    _, _, operation_name = kwargs["event_name"].split(".")
    if readonly:
        if not _PERMITTED_OPERATION_NAMES_RE.search(kwargs["operation_name"]):
            raise Exception(
                f"Operation name {operation_name} did not match {_PERMITTED_OPERATION_NAMES_STR}"
            )
    api_call_stats.increment(account_id, region_name, service_name, operation_name)


class AWSAccessor:
    """AWSAccessor is a wrapper around a boto3 client which provides protection against
    non-Get/List/Describe API calls occurring as well as api call statistic tracking.

    Args:
        session: boto3 Session
        account_id: aws account id
        region_name: aws region
    """

    def __init__(
        self, session: boto3.Session, account_id: str, region_name: str, readonly: bool = True
    ):
        self.session = session
        self.account_id = account_id
        self.region = region_name
        self.api_call_stats = MultilevelCounter()
        self.client_cache: Dict[str, Any] = {}
        self.readonly = readonly

    def client(self, service_name: str) -> BaseClient:
        """Return a boto3 client for a given AWS service_name.

        Args:
            service_name: AWS service name

        Returns:
            boto3 client
        """
        cached_client = self.client_cache.get(service_name)
        if cached_client:
            return cached_client
        client = self.session.client(service_name=service_name, region_name=self.region)
        create_handler = lambda **kwargs: on_request_created(
            api_call_stats=self.api_call_stats,
            account_id=self.account_id,
            region_name=self.region,
            service_name=service_name,
            readonly=self.readonly,
            **kwargs,
        )
        client.meta.events.register("request-created.*.*", create_handler)
        self.client_cache[service_name] = client
        return client
