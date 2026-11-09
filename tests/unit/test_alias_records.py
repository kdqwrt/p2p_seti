
from dataclasses import replace

import msgpack
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
)

from p2p.identity.identity import NodeIdentity
from p2p.dht.alias_records import (
    AliasRecord,
    AliasRecordError,
    alias_record_key,
    create_alias_record,
    validate_alias_record,
)


@pytest.fixture
def identity():
    return NodeIdentity.from_private_key(
        Ed25519PrivateKey.generate()
    )


@pytest.fixture
def record(identity):
    return create_alias_record(
        identity=identity,
        alias="alice",
        sequence_number=1,
        ttl_seconds=180,
    )


def test_create_and_verify(record):
    validate_alias_record(record)
    assert record.alias == "alice"
    assert record.sequence_number == 1
    assert len(record.signature) == 64


def test_serialization_roundtrip(record):
    restored = AliasRecord.from_bytes(record.to_bytes())
    assert restored == record


def test_alias_key_is_deterministic():
    assert alias_record_key("alice") == alias_record_key("alice")
    assert alias_record_key("alice") != alias_record_key("bob")
    assert len(alias_record_key("alice")) == 32


@pytest.mark.parametrize(
    "alias",
    ["", "ab", "Alice", "a b", "-alice", "alice-", "a" * 33, "алиса"],
)
def test_invalid_alias_rejected(identity, alias):
    with pytest.raises(AliasRecordError):
        create_alias_record(identity, alias, sequence_number=1)


def test_modified_alias_invalidates_signature(record):
    modified = replace(record, alias="other")
    with pytest.raises(AliasRecordError):
        validate_alias_record(modified)


def test_modified_sequence_invalidates_signature(record):
    modified = replace(record, sequence_number=2)
    with pytest.raises(AliasRecordError):
        validate_alias_record(modified)


def test_modified_owner_invalidates_record(record):
    modified = replace(record, node_id=b"\x00" * 32)
    with pytest.raises(AliasRecordError):
        validate_alias_record(modified)


def test_invalid_signature_rejected(record):
    modified = replace(record, signature=b"\x00" * 64)
    with pytest.raises(AliasRecordError):
        validate_alias_record(modified)


def test_expired_record_rejected(identity):
    record = create_alias_record(
        identity, "alice", 1, ttl_seconds=10, now=1000
    )
    with pytest.raises(AliasRecordError):
        validate_alias_record(record, now=1010)


def test_future_record_rejected(identity):
    record = create_alias_record(
        identity, "alice", 1, ttl_seconds=180, now=1000
    )
    with pytest.raises(AliasRecordError):
        validate_alias_record(record, now=900)


def test_invalid_ttl_rejected(identity):
    with pytest.raises(AliasRecordError):
        create_alias_record(identity, "alice", 1, ttl_seconds=301)


def test_invalid_sequence_rejected(identity):
    with pytest.raises(AliasRecordError):
        create_alias_record(identity, "alice", -1)


def test_unknown_fields_rejected(record):
    data = record.to_dict()
    data["unknown"] = "value"
    with pytest.raises(AliasRecordError):
        AliasRecord.from_bytes(
            msgpack.packb(data, use_bin_type=True)
        )


def test_noncanonical_encoding_rejected(record):
    data = record.to_dict()
    reordered = dict(reversed(list(data.items())))
    with pytest.raises(AliasRecordError):
        AliasRecord.from_bytes(
            msgpack.packb(reordered, use_bin_type=True)
        )


def test_two_owners_have_same_alias_key():
    assert alias_record_key("alice") == alias_record_key("alice")
