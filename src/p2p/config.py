import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class ConfigError(Exception):
    """Ошибка конфигурации узла."""


@dataclass(slots=True)
class NodeConfig:
    node_state_dir: str
    listen_host: str
    listen_port: int
    bootstrap_peers: list[dict[str, Any]]

    node_id_bits: int
    k_bucket_size: int
    alpha: int

    connect_timeout_ms: int
    read_timeout_ms: int
    ping_timeout_ms: int

    max_frame_payload: int
    protocol_version: int

    log_level: str
    log_file: str

    @classmethod
    def from_file(cls, path: str | Path) -> "NodeConfig":
        config_path = Path(path)

        if not config_path.exists():
            raise ConfigError(
                f"Config file does not exist: {config_path}"
            )

        try:
            with config_path.open(
                "r",
                encoding="utf-8",
            ) as file:
                data = json.load(file)

        except json.JSONDecodeError as exc:
            raise ConfigError(
                f"Invalid JSON in config file: {config_path}"
            ) from exc

        try:
            config = cls(
                node_state_dir=data["node_state_dir"],
                listen_host=data["listen_host"],
                listen_port=data["listen_port"],
                bootstrap_peers=data["bootstrap_peers"],
                node_id_bits=data["node_id_bits"],
                k_bucket_size=data["k_bucket_size"],
                alpha=data["alpha"],
                connect_timeout_ms=data["connect_timeout_ms"],
                read_timeout_ms=data["read_timeout_ms"],
                ping_timeout_ms=data["ping_timeout_ms"],
                max_frame_payload=data["max_frame_payload"],
                protocol_version=data["protocol_version"],
                log_level=data["log_level"],
                log_file=data["log_file"],
            )

        except KeyError as exc:
            raise ConfigError(
                f"Missing required config field: {exc.args[0]}"
            ) from exc

        config.validate()

        return config

    def validate(self) -> None:
        if not isinstance(self.listen_port, int):
            raise ConfigError(
                "listen_port must be an integer"
            )

        if not 1 <= self.listen_port <= 65535:
            raise ConfigError(
                "listen_port must be between 1 and 65535"
            )

        if self.node_id_bits != 256:
            raise ConfigError(
                "node_id_bits must be 256"
            )

        if self.k_bucket_size <= 0:
            raise ConfigError(
                "k_bucket_size must be greater than 0"
            )

        if self.alpha <= 0:
            raise ConfigError(
                "alpha must be greater than 0"
            )

        if self.connect_timeout_ms <= 0:
            raise ConfigError(
                "connect_timeout_ms must be greater than 0"
            )

        if self.read_timeout_ms <= 0:
            raise ConfigError(
                "read_timeout_ms must be greater than 0"
            )

        if self.ping_timeout_ms <= 0:
            raise ConfigError(
                "ping_timeout_ms must be greater than 0"
            )

        if self.max_frame_payload <= 0:
            raise ConfigError(
                "max_frame_payload must be greater than 0"
            )

        if not 0 <= self.protocol_version <= 255:
            raise ConfigError(
                "protocol_version must fit into 1 byte"
            )

        if not isinstance(self.bootstrap_peers, list):
            raise ConfigError(
                "bootstrap_peers must be a list"
            )

    @property
    def connect_timeout(self) -> float:
        return self.connect_timeout_ms / 1000

    @property
    def read_timeout(self) -> float:
        return self.read_timeout_ms / 1000

    @property
    def ping_timeout(self) -> float:
        return self.ping_timeout_ms / 1000