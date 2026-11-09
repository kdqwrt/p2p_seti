
import hashlib
import re
import time
from dataclasses import dataclass, replace
from typing import Any

import msgpack
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PublicKey,
)

from p2p.identity.identity import NodeIdentity, calculate_node_id


ALIAS_DOMAIN = b"p2p-alias-record-v1\x00"
ALIAS_MAX_SIZE = 4096
ALIAS_SIGNATURE_SIZE = 64
ALIAS_MAX_TTL_SECONDS = 300
ALIAS_MAX_CLOCK_SKEW_SECONDS = 30
UINT64_MAX = (1 << 64) - 1

ALIAS_PATTERN = re.compile(r"[a-z0-9](?:[a-z0-9-]{1,30}[a-z0-9])?\Z")


class AliasRecordError(ValueError):
    """Ошибка структуры, подписи или срока действия псевдонима."""


def _validate_alias(alias: Any) -> str:
    if (
        type(alias) is not str
        or not 3 <= len(alias) <= 32
        or ALIAS_PATTERN.fullmatch(alias) is None
    ):
        raise AliasRecordError("Invalid alias")
    return alias


def _validate_uint(value: Any, field: str) -> int:
    if type(value) is not int or not 0 <= value <= UINT64_MAX:
        raise AliasRecordError(f"Invalid {field}")
    return value


def alias_record_key(alias: str) -> bytes:
    """32-байтовый DHT-ключ псевдонима."""
    alias = _validate_alias(alias)
    return hashlib.sha256(
        b"alias:" + alias.encode("ascii")
    ).digest()


@dataclass(frozen=True, slots=True)
class AliasRecord:
    alias: str
    node_id: bytes
    identity_public_key: bytes
    sequence_number: int
    issued_at: int
    expires_at: int
    signature: bytes

    def unsigned_dict(self) -> dict:
        return {
            "alias": self.alias,
            "node_id": self.node_id,
            "identity_public_key": self.identity_public_key,
            "sequence_number": self.sequence_number,
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
        }

    def signing_bytes(self) -> bytes:
        ordered = [
            self.alias,
            self.node_id,
            self.identity_public_key,
            self.sequence_number,
            self.issued_at,
            self.expires_at,
        ]
        return ALIAS_DOMAIN + msgpack.packb(
            ordered, use_bin_type=True
        )

    def to_dict(self) -> dict:
        return {
            **self.unsigned_dict(),
            "signature": self.signature,
        }

    def to_bytes(self) -> bytes:
        return msgpack.packb(
            self.to_dict(), use_bin_type=True
        )

    @classmethod
    def from_dict(cls, data: dict) -> "AliasRecord":
        required = {
            "alias",
            "node_id",
            "identity_public_key",
            "sequence_number",
            "issued_at",
            "expires_at",
            "signature",
        }
        if type(data) is not dict or set(data) != required:
            raise AliasRecordError("Invalid AliasRecord fields")

        record = cls(
            alias=_validate_alias(data["alias"]),
            node_id=data["node_id"],
            identity_public_key=data["identity_public_key"],
            sequence_number=_validate_uint(
                data["sequence_number"], "sequence_number"
            ),
            issued_at=_validate_uint(data["issued_at"], "issued_at"),
            expires_at=_validate_uint(data["expires_at"], "expires_at"),
            signature=data["signature"],
        )
        return record

    @classmethod
    def from_bytes(cls, data: bytes) -> "AliasRecord":
        if (
            type(data) is not bytes
            or not 0 < len(data) <= ALIAS_MAX_SIZE
        ):
            raise AliasRecordError("Invalid record size")

        try:
            unpacked = msgpack.unpackb(
                data,
                raw=False,
                strict_map_key=True,
                max_map_len=7,
                max_array_len=7,
                max_str_len=256,
                max_bin_len=ALIAS_MAX_SIZE,
            )
        except (ValueError, TypeError, msgpack.UnpackException) as exc:
            raise AliasRecordError(
                "Cannot decode AliasRecord"
            ) from exc

        record = cls.from_dict(unpacked)

        if record.to_bytes() != data:
            raise AliasRecordError(
                "Non-canonical AliasRecord encoding"
            )

        validate_alias_record(record)
        return record


def validate_alias_record(
    record: AliasRecord,
    now: int | None = None,
) -> None:
    """Проверяет запись, идентичность, подпись и TTL."""
    if not isinstance(record, AliasRecord):
        raise AliasRecordError("Expected AliasRecord")

    _validate_alias(record.alias)

    if type(record.node_id) is not bytes or len(record.node_id) != 32:
        raise AliasRecordError("Invalid NodeID")

    if (
        type(record.identity_public_key) is not bytes
        or len(record.identity_public_key) != 32
    ):
        raise AliasRecordError("Invalid public key")

    try:
        expected_id = calculate_node_id(
            record.identity_public_key
        )
    except (ValueError, TypeError) as exc:
        raise AliasRecordError(
            "Invalid identity public key"
        ) from exc

    if record.node_id != expected_id:
        raise AliasRecordError(
            "NodeID does not match public key"
        )

    _validate_uint(record.sequence_number, "sequence_number")
    _validate_uint(record.issued_at, "issued_at")
    _validate_uint(record.expires_at, "expires_at")

    ttl = record.expires_at - record.issued_at
    if not 1 <= ttl <= ALIAS_MAX_TTL_SECONDS:
        raise AliasRecordError("Invalid TTL")

    current = (
        int(time.time())
        if now is None
        else _validate_uint(now, "now")
    )

    if record.issued_at > current + ALIAS_MAX_CLOCK_SKEW_SECONDS:
        raise AliasRecordError("Record issued in the future")

    if record.expires_at <= current:
        raise AliasRecordError("Record has expired")

    if (
        type(record.signature) is not bytes
        or len(record.signature) != ALIAS_SIGNATURE_SIZE
    ):
        raise AliasRecordError("Invalid signature size")

    if len(record.to_bytes()) > ALIAS_MAX_SIZE:
        raise AliasRecordError("Record exceeds maximum size")

    try:
        public_key = Ed25519PublicKey.from_public_bytes(
            record.identity_public_key
        )
        public_key.verify(
            record.signature,
            record.signing_bytes(),
        )
    except (InvalidSignature, ValueError) as exc:
        raise AliasRecordError(
            "Invalid AliasRecord signature"
        ) from exc


def create_alias_record(
    identity: NodeIdentity,
    alias: str,
    sequence_number: int,
    ttl_seconds: int = 180,
    now: int | None = None,
) -> AliasRecord:
    """Создаёт и подписывает псевдоним владельца."""
    _validate_alias(alias)
    _validate_uint(sequence_number, "sequence_number")

    if (
        type(ttl_seconds) is not int
        or not 1 <= ttl_seconds <= ALIAS_MAX_TTL_SECONDS
    ):
        raise AliasRecordError("Invalid TTL")

    issued_at = (
        int(time.time())
        if now is None
        else _validate_uint(now, "now")
    )

    if issued_at + ttl_seconds > UINT64_MAX:
        raise AliasRecordError("Timestamp overflow")

    if not isinstance(identity, NodeIdentity):
        raise AliasRecordError("Expected NodeIdentity")

    record = AliasRecord(
        alias=alias,
        node_id=identity.node_id,
        identity_public_key=identity.public_key,
        sequence_number=sequence_number,
        issued_at=issued_at,
        expires_at=issued_at + ttl_seconds,
        signature=b"",
    )

    signed = replace(
        record,
        signature=identity.sign(record.signing_bytes()),
    )

    validate_alias_record(signed, now=issued_at)
    return signed
