
from dataclasses import replace

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
)

from p2p.dht.records import (
    NodeRecord,
    RecordError,
    create_node_record,
    node_record_key,
    validate_node_record,
)
from p2p.identity.identity import NodeIdentity


@pytest.fixture
def identity():
    return NodeIdentity.from_private_key(
        Ed25519PrivateKey.generate()
    )


@pytest.fixture
def record(identity):
    return create_node_record(
        identity,
        addresses=["127.0.0.1:9101"],
        sequence_number=1,
        ttl_seconds=180,
        now=1000,
    )


def test_create_and_verify(record):
    validate_node_record(record, now=1000)


def test_serialization_roundtrip(record):
    restored = NodeRecord.from_bytes(record.to_bytes())

    assert restored == record
    validate_node_record(restored, now=1000)


def test_node_id_mismatch(record):
    changed = replace(record, node_id=b"\x00" * 32)

    with pytest.raises(RecordError):
        validate_node_record(changed, now=1000)


def test_modified_address_invalidates_signature(record):
    changed = replace(
        record,
        addresses=("127.0.0.1:9999",),
    )

    with pytest.raises(RecordError, match="signature"):
        validate_node_record(changed, now=1000)


def test_modified_sequence_invalidates_signature(record):
    changed = replace(record, sequence_number=2)

    with pytest.raises(RecordError, match="signature"):
        validate_node_record(changed, now=1000)


def test_modified_expiration_invalidates_signature(record):
    changed = replace(record, expires_at=1100)

    with pytest.raises(RecordError, match="signature"):
        validate_node_record(changed, now=1000)


def test_expired_record_rejected(record):
    with pytest.raises(RecordError, match="expired"):
        validate_node_record(record, now=1180)


def test_future_record_rejected(record):
    with pytest.raises(RecordError, match="future"):
        validate_node_record(record, now=900)


def test_invalid_address_rejected(identity):
    with pytest.raises(RecordError):
        create_node_record(
            identity,
            addresses=["not-an-ip:9101"],
            sequence_number=1,
            now=1000,
        )


def test_invalid_ttl_rejected(identity):
    with pytest.raises(RecordError):
        create_node_record(
            identity,
            addresses=["127.0.0.1:9101"],
            sequence_number=1,
            ttl_seconds=301,
            now=1000,
        )


def test_duplicate_addresses_rejected(identity):
    with pytest.raises(RecordError):
        create_node_record(
            identity,
            addresses=[
                "127.0.0.1:9101",
                "127.0.0.1:9101",
            ],
            sequence_number=1,
            now=1000,
        )


def test_invalid_signature_rejected(record):
    changed = replace(record, signature=b"\x00" * 64)

    with pytest.raises(RecordError, match="signature"):
        validate_node_record(changed, now=1000)


def test_dht_key_is_deterministic(identity):
    key1 = node_record_key(identity.node_id)
    key2 = node_record_key(identity.node_id)

    assert key1 == key2
    assert len(key1) == 32
    assert key1 != identity.node_id


def test_unknown_fields_rejected(record):
    payload = record.to_dict()
    payload["unexpected"] = True

    with pytest.raises(RecordError):
        NodeRecord.from_dict(payload)
