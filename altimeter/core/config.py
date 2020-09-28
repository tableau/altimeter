"""Configuration classes"""
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Type, Tuple

import boto3
import toml

from altimeter.aws.auth.accessor import Accessor
from altimeter.core.artifact_io import is_s3_uri, parse_s3_uri


class InvalidConfigException(Exception):
    """Indicates an invalid configuration"""


def _get_required_param(key: str, config_dict: Dict[str, Any]) -> Any:
    """Get a parameter by key from a config dict. Raise InvalidConfigException if it does
    not exist."""
    value = config_dict.get(key)
    if value is None:
        raise InvalidConfigException(f"Missing parameter '{key}'")
    return value


def _get_optional_param(key: str, config_dict: Dict[str, Any]) -> Any:
    """Get a parameter by key from a config dict. Return None if it does not exist."""
    return config_dict.get(key)


def get_required_list_param(key: str, config_dict: Dict[str, Any]) -> Tuple[Any, ...]:
    """Get a list parameter by key from a config dict. Raise InvalidConfigException if it does
    not exist or its value is not a list. Return as a tuple."""
    value = _get_required_param(key, config_dict)
    if isinstance(value, tuple):
        return value
    if not isinstance(value, list):
        raise InvalidConfigException(
            f"Parameter '{key}' should be a list or tuple. Is {type(value)}"
        )
    return tuple(value)


def get_required_bool_param(key: str, config_dict: Dict[str, Any]) -> bool:
    """Get a bool parameter by key from a config dict. Raise InvalidConfigException if it does
    not exist or its value is not a bool."""
    value = _get_required_param(key, config_dict)
    if not isinstance(value, bool):
        raise InvalidConfigException(f"Parameter '{key}' should be a bool. Is {type(value)}")
    return value


def get_optional_bool_param(key: str, config_dict: Dict[str, Any]) -> Optional[bool]:
    """Get a bool parameter by key from a config dict. Return None if it does not exist,
    raise InvalidConfigException if its value is not a bool."""
    value = _get_optional_param(key, config_dict)
    if value is None:
        return None
    if not isinstance(value, bool):
        raise InvalidConfigException(f"Parameter '{key}' should be a bool. Is {type(value)}")
    return value


def get_optional_str_param(key: str, config_dict: Dict[str, Any]) -> Optional[str]:
    """Get a str parameter by key from a config dict. Return None if it does not exist,
    raise InvalidConfigException if its value is not a bool."""
    value = _get_optional_param(key, config_dict)
    if value is None:
        return None
    if not isinstance(value, str):
        raise InvalidConfigException(f"Parameter '{key}' should be a str. Is {type(value)}")
    return value


def get_required_int_param(key: str, config_dict: Dict[str, Any]) -> int:
    """Get a int parameter by key from a config dict. Raise InvalidConfigException if it does
    not exist or its value is not a int."""
    value = _get_required_param(key, config_dict)
    if not isinstance(value, int):
        raise InvalidConfigException(f"Parameter '{key}' should be a int. Is {type(value)}")
    return value


def get_required_str_param(key: str, config_dict: Dict[str, Any]) -> str:
    """Get a str parameter by key from a config dict. Raise InvalidConfigException if it does
    not exist or its value is not a str."""
    value = _get_required_param(key, config_dict)
    if not isinstance(value, str):
        raise InvalidConfigException(f"Parameter '{key}' should be a str. Is {type(value)}")
    return value


def get_required_section(key: str, config_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Get a section from a config dict. Raise InvalidConfigException if it does not exist."""
    value = config_dict.get(key)
    if value is None:
        raise InvalidConfigException(f"Missing section '{key}'")
    if not isinstance(value, dict):
        raise InvalidConfigException(f"'{key}' does not appear to be a section. Is {type(value)}")
    return value


def get_optional_section(key: str, config_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Get a section from a config dict. Return None if it does not exist."""
    value = config_dict.get(key)
    if value is not None:
        if not isinstance(value, dict):
            raise InvalidConfigException(
                f"'{key}' does not appear to be a section. Is {type(value)}"
            )
    return value


@dataclass(frozen=True)
class AccessConfig:
    """Access configuration class"""

    accessor: Accessor

    @classmethod
    def from_dict(cls: Type["AccessConfig"], config_dict: Dict[str, Any]) -> "AccessConfig":
        """Build an AccessConfig from a dict"""
        cache_creds = get_optional_bool_param("cache_creds", config_dict)
        if cache_creds is not None:
            accessor = Accessor.from_dict(config_dict, cache_creds=cache_creds)
        else:
            accessor = Accessor.from_dict(config_dict)
        return AccessConfig(accessor=accessor)


@dataclass(frozen=True)
class ScanConfig:
    """Scan configuration class"""

    accounts: Tuple[str, ...]
    regions: Tuple[str, ...]
    scan_sub_accounts: bool
    preferred_account_scan_regions: Tuple[str, ...]
    single_account_mode: bool
    scan_lambda_tcp_keepalive: Optional[bool] = False

    @classmethod
    def from_dict(cls: Type["ScanConfig"], config_dict: Dict[str, Any]) -> "ScanConfig":
        """Build a ScanConfig from a dict"""
        preferred_account_scan_regions = get_required_list_param(
            "preferred_account_scan_regions", config_dict
        )
        accounts = get_required_list_param("accounts", config_dict)
        regions = get_required_list_param("regions", config_dict)
        scan_sub_accounts = get_required_bool_param("scan_sub_accounts", config_dict)
        if accounts:
            single_account_mode = False
        else:
            single_account_mode = True
        if not accounts:
            sts_client = boto3.client("sts")
            account_id = sts_client.get_caller_identity()["Account"]
            accounts = (account_id,)

        return ScanConfig(
            accounts=accounts,
            regions=regions,
            scan_sub_accounts=scan_sub_accounts,
            preferred_account_scan_regions=preferred_account_scan_regions,
            single_account_mode=single_account_mode,
        )


@dataclass(frozen=True)
class ConcurrencyConfig:
    """Concurrency configuration class"""

    max_account_scan_threads: int
    max_accounts_per_thread: int
    max_svc_scan_threads: int

    @classmethod
    def from_dict(
        cls: Type["ConcurrencyConfig"], config_dict: Dict[str, Any]
    ) -> "ConcurrencyConfig":
        """Build a ConcurrencyConfig from a dict"""
        max_account_scan_threads = get_required_int_param("max_account_scan_threads", config_dict)
        max_accounts_per_thread = get_required_int_param("max_accounts_per_thread", config_dict)
        max_svc_scan_threads = get_required_int_param("max_svc_scan_threads", config_dict)
        return ConcurrencyConfig(
            max_account_scan_threads=max_account_scan_threads,
            max_accounts_per_thread=max_accounts_per_thread,
            max_svc_scan_threads=max_svc_scan_threads,
        )


@dataclass(frozen=True)
class NeptuneConfig:
    """Neptune configuration class"""

    host: str
    port: int
    region: str
    iam_role_arn: Optional[str] = ""
    graph_load_sns_topic_arn: Optional[str] = ""
    ssl: Optional[bool] = True
    use_lpg: Optional[bool] = False
    iam_credentials_provider_type: Optional[str] = ""
    auth_mode: Optional[str] = ""

    @classmethod
    def from_dict(cls: Type["NeptuneConfig"], config_dict: Dict[str, Any]) -> "NeptuneConfig":
        """Build a NeptuneConfig from a dict"""
        host = get_required_str_param("host", config_dict)
        port = get_required_int_param("port", config_dict)
        region = get_required_str_param("region", config_dict)
        ssl = get_optional_bool_param("ssl", config_dict)
        use_lpg = get_optional_bool_param("use_lpg", config_dict)
        iam_role_arn = get_optional_str_param("iam_role_arn", config_dict)
        auth_mode = get_optional_str_param("auth_mode", config_dict)
        graph_load_sns_topic_arn = get_optional_str_param("graph_load_sns_topic_arn", config_dict)
        return NeptuneConfig(
            host=host,
            port=port,
            region=region,
            ssl=ssl,
            use_lpg=use_lpg,
            iam_role_arn=iam_role_arn,
            graph_load_sns_topic_arn=graph_load_sns_topic_arn,
            auth_mode=auth_mode,
        )


@dataclass(frozen=True)
class Config:
    """Top level configuration class"""

    artifact_path: str
    pruner_max_age_min: int
    graph_name: str
    access: AccessConfig
    concurrency: ConcurrencyConfig
    scan: ScanConfig
    neptune: Optional[NeptuneConfig] = None

    def __post_init__(self) -> None:
        if (
            self.scan.single_account_mode
            and not self.scan.scan_sub_accounts
            and self.access.accessor.multi_hop_accessors
        ):
            raise InvalidConfigException("Accessor config not supported for single account mode")
        if is_s3_uri(self.artifact_path):
            bucket, key_prefix = parse_s3_uri(self.artifact_path)
            if key_prefix is not None:
                raise InvalidConfigException(
                    f"S3 artifact_path should be s3://<bucket>, no key - got {self.artifact_path}"
                )

    @classmethod
    def from_dict(cls: Type["Config"], config_dict: Dict[str, Any]) -> "Config":
        """Build a Config from a dict"""
        artifact_path = get_required_str_param("artifact_path", config_dict)
        pruner_max_age_min = get_required_int_param("pruner_max_age_min", config_dict)
        graph_name = get_required_str_param("graph_name", config_dict)

        scan_dict = get_required_section("scan", config_dict)
        try:
            scan = ScanConfig.from_dict(scan_dict)
        except InvalidConfigException as ice:
            raise InvalidConfigException(f"{str(ice)} in section 'scan'")

        concurrency_dict = get_required_section("concurrency", config_dict)
        try:
            concurrency = ConcurrencyConfig.from_dict(concurrency_dict)
        except InvalidConfigException as ice:
            raise InvalidConfigException(f"{str(ice)} in section 'concurrency'")

        access_dict = get_required_section("access", config_dict)
        try:
            access = AccessConfig.from_dict(access_dict)
        except InvalidConfigException as ice:
            raise InvalidConfigException(f"{str(ice)} in section 'access'")

        neptune_dict = get_optional_section("neptune", config_dict)
        neptune: Optional[NeptuneConfig] = None
        if neptune_dict:
            try:
                neptune = NeptuneConfig.from_dict(neptune_dict)
            except InvalidConfigException as ice:
                raise InvalidConfigException(f"{str(ice)} in section 'neptune'")
        return Config(
            artifact_path=artifact_path,
            pruner_max_age_min=pruner_max_age_min,
            graph_name=graph_name,
            access=access,
            concurrency=concurrency,
            scan=scan,
            neptune=neptune,
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
            return cls.from_dict(config_dict)
        except InvalidConfigException as ice:
            raise InvalidConfigException(f"Error in conf file {filepath}: {str(ice)}")

    @classmethod
    def from_s3(cls: Type["Config"], s3_uri: str) -> "Config":
        """Load a Config from an s3 object"""
        bucket, key = parse_s3_uri(s3_uri)
        s3_client = boto3.client("s3")
        resp = s3_client.get_object(Bucket=bucket, Key=key,)
        config_str = resp["Body"].read().decode("utf-8")
        config_dict = dict(toml.loads(config_str))
        try:
            return cls.from_dict(config_dict)
        except InvalidConfigException as ice:
            raise InvalidConfigException(f"Error in conf file {s3_uri}: {str(ice)}")
