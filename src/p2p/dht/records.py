
import hashlib
import ipaddress
import time
from dataclasses import dataclass
from typing import Any

import msgpack
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PublicKey,
)

from p2p.identity.identity import (
    NodeIdentity,
    calculate_node_id,
)


RECORD_DOMAIN = b"p2p-node-record-v1\x00"
RECORD_MAX_SIZE = 4096
RECORD_SIGNATURE_SIZE = 64
MAX_ADDRESSES = 8
MAX_TTL_SECONDS = 300
MAX_CLOCK_SKEW_SECONDS = 30
UINT64_MAX = (1 << 64) - 1


class RecordError(ValueError):
    """Недействительная или некорректная DHT-запись."""


def _uint(value: Any, name: str) -> int:
    if type(value) is not int or not 0 <= value <= UINT64_MAX:
        raise RecordError(f"Invalid {name}")
    return value


def _addresses(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise RecordError("Addresses must be a list")

    if not 1 <= len(value) <= MAX_ADDRESSES:
        raise RecordError("Invalid number of addresses")

    result = []

    for address in value:
        if not isinstance(address, str):
            raise RecordError("Address must be a string")

        # Формат: IPv4:port или [IPv6]:port.
        if address.startswith("["):
            end = address.find("]")
            if end == -1 or address[end + 1:end + 2] != ":":
                raise RecordError("Invalid IPv6 address")
            host = address[1:end]
            port_text = address[end + 2:]
        else:
            if address.count(":") != 1:
                raise RecordError("Invalid address")
            host, port_text = address.rsplit(":", 1)

        try:
            ip = ipaddress.ip_address(host)
            port = int(port_text)
        except (ValueError, TypeError) as exc:
            raise RecordError("Invalid address or port") from exc

        if not port_text.isascii() or not port_text.isdecimal():
            raise RecordError("Invalid port")

        if not 1 <= port <= 65535:
            raise RecordError("Port out of range")

        canonical = (
            f"[{ip.compressed}]:{port}"
            if ip.version == 6
            else f"{ip.compressed}:{port}"
        )

        if address != canonical:
            raise RecordError("Non-canonical address")

        result.append(canonical)

    if len(set(result)) != len(result):
        raise RecordError("Duplicate addresses")

    if result != sorted(result):
        raise RecordError("Addresses must be sorted")

    return tuple(result)


@dataclass(frozen=True, slots=True)
class NodeRecord:
    node_id: bytes
    identity_public_key: bytes
    addresses: tuple[str, ...]
    sequence_number: int
    issued_at: int
    expires_at: int
    signature: bytes

    def unsigned_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "identity_public_key": self.identity_public_key,
            "addresses": list(self.addresses),
            "sequence_number": self.sequence_number,
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
        }

    def signing_bytes(self) -> bytes:
        fields = self.unsigned_dict()

        # Массив фиксированного порядка исключает зависимость
        # подписи от порядка ключей словаря MessagePack.
        ordered = [
            fields["node_id"],
            fields["identity_public_key"],
            fields["addresses"],
            fields["sequence_number"],
            fields["issued_at"],
            fields["expires_at"],
        ]

        return RECORD_DOMAIN + msgpack.packb(
            ordered,
            use_bin_type=True,
        )

    def to_dict(self) -> dict:
        return {
            **self.unsigned_dict(),
            "signature": self.signature,
        }

    def to_bytes(self) -> bytes:
        return msgpack.packb(
            self.to_dict(),
            use_bin_type=True,
        )

    @classmethod
    def from_dict(cls, data: dict) -> "NodeRecord":
        required = {
            "node_id",
            "identity_public_key",
            "addresses",
            "sequence_number",
            "issued_at",
            "expires_at",
            "signature",
        }

        if not isinstance(data, dict) or set(data) != required:
            raise RecordError("Invalid NodeRecord fields")

        try:
            return cls(
                node_id=data["node_id"],
                identity_public_key=data["identity_public_key"],
                addresses=_addresses(data["addresses"]),
                sequence_number=_uint(
                    data["sequence_number"], "sequence_number"
                ),
                issued_at=_uint(data["issued_at"], "issued_at"),
                expires_at=_uint(data["expires_at"], "expires_at"),
                signature=data["signature"],
            )
        except (TypeError, KeyError) as exc:
            raise RecordError("Invalid NodeRecord") from exc

    @classmethod
    def from_bytes(cls, data: bytes) -> "NodeRecord":
        if not isinstance(data, bytes) or len(data) > RECORD_MAX_SIZE:
            raise RecordError("Invalid record size")

        try:
            unpacked = msgpack.unpackb(
                data,
                raw=False,
                strict_map_key=True,
                max_array_len=MAX_ADDRESSES,
                max_map_len=7,
                max_str_len=256,
                max_bin_len=RECORD_MAX_SIZE,
            )
        except (ValueError, TypeError, msgpack.UnpackException) as exc:
            raise RecordError("Cannot decode NodeRecord") from exc

        record = cls.from_dict(unpacked)

        if record.to_bytes() != data:
            raise RecordError("Non-canonical record encoding")

        return record


def create_node_record(
    identity: NodeIdentity,
    addresses: list[str] | tuple[str, ...],
    sequence_number: int,
    ttl_seconds: int = 180,
    now: int | None = None,
) -> NodeRecord:
    """Создаёт запись и подписывает её ключом владельца."""

    if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= MAX_TTL_SECONDS:
        raise RecordError("Invalid TTL")

    issued_at = int(time.time()) if now is None else _uint(now, "now")

    if issued_at + ttl_seconds > UINT64_MAX:
        raise RecordError("Timestamp overflow")

    record = NodeRecord(
        node_id=identity.node_id,
        identity_public_key=identity.public_key,
        addresses=_addresses(addresses),
        sequence_number=_uint(sequence_number, "sequence_number"),
        issued_at=issued_at,
        expires_at=issued_at + ttl_seconds,
        signature=b"",
    )

    signature = identity.sign(record.signing_bytes())

    signed = NodeRecord(
        node_id=record.node_id,
        identity_public_key=record.identity_public_key,
        addresses=record.addresses,
        sequence_number=record.sequence_number,
        issued_at=record.issued_at,
        expires_at=record.expires_at,
        signature=signature,
    )

    validate_node_record(signed, now=issued_at)
    return signed


def validate_node_record(
    record: NodeRecord,
    now: int | None = None,
) -> None:
    """Проверяет структуру, идентичность, подпись и время действия."""

    if not isinstance(record, NodeRecord):
        raise RecordError("Expected NodeRecord")

    if not isinstance(record.node_id, bytes) or len(record.node_id) != 32:
        raise RecordError("Invalid NodeID")

    if not isinstance(record.identity_public_key, bytes):
        raise RecordError("Invalid public key")

    try:
        expected_id = calculate_node_id(record.identity_public_key)
    except (ValueError, Exception) as exc:
        raise RecordError("Invalid identity public key") from exc

    if record.node_id != expected_id:
        raise RecordError("NodeID does not match public key")

    _addresses(record.addresses)
    _uint(record.sequence_number, "sequence_number")
    _uint(record.issued_at, "issued_at")
    _uint(record.expires_at, "expires_at")

    if record.expires_at <= record.issued_at:
        raise RecordError("Invalid record lifetime")

    if record.expires_at - record.issued_at > MAX_TTL_SECONDS:
        raise RecordError("TTL exceeds maximum")

    current = int(time.time()) if now is None else _uint(now, "now")

    if record.issued_at > current + MAX_CLOCK_SKEW_SECONDS:
        raise RecordError("Record issued in the future")

    if record.expires_at <= current:
        raise RecordError("Record has expired")

    if (
        not isinstance(record.signature, bytes)
        or len(record.signature) != RECORD_SIGNATURE_SIZE
    ):
        raise RecordError("Invalid signature size")

    if len(record.to_bytes()) > RECORD_MAX_SIZE:
        raise RecordError("Record exceeds maximum size")

    try:
        public_key = Ed25519PublicKey.from_public_bytes(
            record.identity_public_key
        )
        public_key.verify(
            record.signature,
            record.signing_bytes(),
        )
    except (InvalidSignature, ValueError) as exc:
        raise RecordError("Invalid record signature") from exc


def node_record_key(node_id: bytes) -> bytes:
    """DHT-ключ записи по NodeID."""

    if not isinstance(node_id, bytes) or len(node_id) != 32:
        raise RecordError("Invalid NodeID")

    return hashlib.sha256(b"node:" + node_id).digest()
