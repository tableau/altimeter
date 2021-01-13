"""Configuration classes"""
from pathlib import Path
from typing import Any, Optional, Type, Tuple

import boto3
from pydantic import Field
import toml

from altimeter.aws.auth.accessor import Accessor
from altimeter.core.artifact_io import is_s3_uri, parse_s3_uri
from altimeter.core.base_model import BaseImmutableModel


class InvalidConfigException(Exception):
    """Indicates an invalid configuration"""


class ScanConfig(BaseImmutableModel):
    """Scan configuration class"""

    accounts: Tuple[str, ...]
    regions: Tuple[str, ...]
    scan_sub_accounts: bool
    preferred_account_scan_regions: Tuple[str, ...]


class ConcurrencyConfig(BaseImmutableModel):
    """Concurrency configuration class"""

    max_account_scan_threads: int
    max_svc_scan_threads: int


class NeptuneConfig(BaseImmutableModel):
    """Neptune configuration class"""

    host: str
    port: int
    region: str
    iam_role_arn: Optional[str]
    graph_load_sns_topic_arn: Optional[str]
    ssl: Optional[bool] = True
    use_lpg: Optional[bool] = False
    iam_credentials_provider_type: Optional[str]
    auth_mode: Optional[str]


class Config(BaseImmutableModel):
    """Top level configuration class"""

    artifact_path: str
    pruner_max_age_min: int
    graph_name: str
    concurrency: ConcurrencyConfig
    scan: ScanConfig
    accessor: Accessor = Field(default_factory=Accessor)
    write_master_json: bool = False
    neptune: Optional[NeptuneConfig] = None

    def __init__(self, **data: Any):
        super().__init__(**data)
        if (
            not self.scan.accounts
            and not self.scan.scan_sub_accounts
            and self.accessor.multi_hop_accessors
        ):
            raise InvalidConfigException("Accessor config not supported for single account mode")
        if is_s3_uri(self.artifact_path):
            _, key_prefix = parse_s3_uri(self.artifact_path)
            if key_prefix is not None:
                raise InvalidConfigException(
                    f"S3 artifact_path should be s3://<bucket>, no key - got {self.artifact_path}"
                )

    @classmethod
    def from_path(cls: Type["Config"], path: str) -> "Config":
        """Load a Config from an s3 uri or a  file"""
        if is_s3_uri(path):
            return cls.from_s3(s3_uri=path)
        return cls.from_file(filepath=Path(path))

    @classmethod
    def from_file(cls: Type["Config"], filepath: Path) -> "Config":
        """Load a Config from a file"""
        with open(filepath, "r") as fp:
            config_str = fp.read()
        config_dict = dict(toml.loads(config_str))
        try:
            return cls(**config_dict)
        except InvalidConfigException as ice:
            raise InvalidConfigException(f"Error in conf file {filepath}: {str(ice)}") from ice

    @classmethod
    def from_s3(cls: Type["Config"], s3_uri: str) -> "Config":
        """Load a Config from an s3 object"""
        bucket, key = parse_s3_uri(s3_uri)
        s3_client = boto3.client("s3")
        resp = s3_client.get_object(Bucket=bucket, Key=key,)
        config_str = resp["Body"].read().decode("utf-8")
        config_dict = dict(toml.loads(config_str))
        try:
            return cls(**config_dict)
        except InvalidConfigException as ice:
            raise InvalidConfigException(f"Error in conf file {s3_uri}: {str(ice)}") from ice
