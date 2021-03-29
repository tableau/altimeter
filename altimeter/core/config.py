"""Configuration classes"""
from pathlib import Path
from typing import Any, Optional, Type, Tuple, TypeVar

import boto3
from pydantic import Field, BaseSettings
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
    max_account_scan_tries: int = 2


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


GenericConfig = TypeVar("GenericConfig", bound="Config")


class Config(BaseImmutableModel):
    """Config class to be overridden by graphers"""

    artifact_path: str
    pruner_max_age_min: int
    graph_name: str
    neptune: Optional[NeptuneConfig] = None

    class Config:
        """Pydantic config"""

        allow_mutation = False
        extra = "ignore"
        arbitrary_types_allowed = True

    def __init__(self, **data: Any):
        super().__init__(**data)
        if is_s3_uri(self.artifact_path):
            parse_s3_uri(self.artifact_path)

    @classmethod
    def from_path(cls: Type[GenericConfig], path: str) -> GenericConfig:
        """Load a Config from an s3 uri or a file"""
        if is_s3_uri(path):
            return cls.from_s3(s3_uri=path)
        return cls.from_file(filepath=Path(path))

    @classmethod
    def from_file(cls: Type[GenericConfig], filepath: Path) -> GenericConfig:
        """Load a Config from a file"""
        with open(filepath, "r") as fp:
            config_str = fp.read()
        config_dict = dict(toml.loads(config_str))
        try:
            return cls(**config_dict)
        except InvalidConfigException as ice:
            raise InvalidConfigException(f"Error in conf file {filepath}: {str(ice)}") from ice

    @classmethod
    def from_s3(cls: Type[GenericConfig], s3_uri: str) -> GenericConfig:
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


class AWSConfig(Config):
    """Top level configuration class"""

    concurrency: ConcurrencyConfig
    scan: ScanConfig
    accessor: Accessor = Field(default_factory=Accessor)
    write_master_json: bool = False
    services_regions_json_url: str = "https://api.regional-table.region-services.aws.a2z.com/index.json"

    def __init__(self, **data: Any):
        super().__init__(**data)
        if (
            not self.scan.accounts
            and not self.scan.scan_sub_accounts
            and self.accessor.multi_hop_accessors
        ):
            raise InvalidConfigException("Accessor config not supported for single account mode")


class GraphPrunerConfig(BaseSettings):
    config_path: str
