"""Configuration classes"""
from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any, Dict, Optional, Type, Tuple

import boto3
import jinja2
import toml

from altimeter.aws.auth.accessor import Accessor


class InvalidConfigException(Exception):
    """Indicates an invalid configuration"""


def _get_required_param(key: str, config_dict: Dict[str, Any]) -> Any:
    """Get a parameter by key from a config dict. Raise InvalidConfigException if it does
    not exist."""
    value = config_dict.get(key)
    if value is None:
        raise InvalidConfigException(f"Missing parameter '{key}'")
    return value


def get_required_list_param(key: str, config_dict: Dict[str, Any]) -> Tuple[Any, ...]:
    """Get a list parameter by key from a config dict. Raise InvalidConfigException if it does
    not exist or its value is not a list. Return as a tuple."""
    value = _get_required_param(key, config_dict)
    if not isinstance(value, list):
        raise InvalidConfigException(f"Parameter '{key}' should be a list. Is {type(value)}")
    return tuple(value)


def get_required_bool_param(key: str, config_dict: Dict[str, Any]) -> bool:
    """Get a bool parameter by key from a config dict. Raise InvalidConfigException if it does
    not exist or its value is not a bool."""
    value = _get_required_param(key, config_dict)
    if not isinstance(value, bool):
        raise InvalidConfigException(f"Parameter '{key}' should be a bool. Is {type(value)}")
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
        cache_creds = get_required_bool_param("cache_creds", config_dict)
        accessor = Accessor.from_dict(config_dict, cache_creds=cache_creds)
        return AccessConfig(accessor=accessor)


@dataclass(frozen=True)
class ScanConfig:
    """Scan configuration class"""

    accounts: Tuple[str, ...]
    regions: Tuple[str, ...]
    scan_sub_accounts: bool
    preferred_account_scan_regions: Tuple[str, ...]
    single_account_mode: bool

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
    iam_role: str

    @classmethod
    def from_dict(cls: Type["NeptuneConfig"], config_dict: Dict[str, Any]) -> "NeptuneConfig":
        """Build a NeptuneConfig from a dict"""
        host = get_required_str_param("host", config_dict)
        port = get_required_int_param("port", config_dict)
        region = get_required_str_param("region", config_dict)
        iam_role = get_required_str_param("iam_role", config_dict)
        return NeptuneConfig(host=host, port=port, region=region, iam_role=iam_role,)


@dataclass(frozen=True)
class Config:
    """Top level configuration class"""

    access: AccessConfig
    concurrency: ConcurrencyConfig
    scan: ScanConfig
    artifact_path: str
    neptune: Optional[NeptuneConfig] = None

    def __post_init__(self) -> None:
        if (
            self.scan.single_account_mode
            and not self.scan.scan_sub_accounts
            and self.access.accessor.multi_hop_accessors
        ):
            raise InvalidConfigException("Accessor config not supported for single account mode")

    @classmethod
    def from_dict(cls: Type["Config"], config_dict: Dict[str, Any]) -> "Config":
        """Build a Config from a dict"""
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
        artifact_path = get_required_str_param("artifact_path", config_dict)
        template = jinja2.Environment(
            loader=jinja2.BaseLoader(), undefined=jinja2.StrictUndefined
        ).from_string(artifact_path)
        artifact_path = template.render(env=os.environ)

        neptune_dict = get_optional_section("neptune", config_dict)
        neptune: Optional[NeptuneConfig] = None
        if neptune_dict:
            try:
                neptune = NeptuneConfig.from_dict(neptune_dict)
            except InvalidConfigException as ice:
                raise InvalidConfigException(f"{str(ice)} in section 'neptune'")
        return Config(
            access=access,
            concurrency=concurrency,
            scan=scan,
            artifact_path=artifact_path,
            neptune=neptune,
        )

    @classmethod
    def from_file(cls: Type["Config"], filepath: Path) -> "Config":
        """Load a Config from a file"""
        config_dict = dict(toml.load(filepath))
        try:
            return cls.from_dict(config_dict)
        except InvalidConfigException as ice:
            raise InvalidConfigException(f"Error in conf file {filepath}: {str(ice)}")
