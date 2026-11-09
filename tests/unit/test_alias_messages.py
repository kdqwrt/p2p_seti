
import pytest

from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
)

from p2p.identity.identity import NodeIdentity
from p2p.dht.alias_records import (
    create_alias_record,
    alias_record_key,
)
from p2p.dht.alias_messages import (
    encode_store_alias_request,
    decode_store_alias_request,
    encode_store_alias_response,
    decode_store_alias_response,
    encode_find_alias_request,
    decode_find_alias_request,
    encode_find_alias_response,
    decode_find_alias_response,
)
from p2p.protocol.payload_codec import PayloadError


@pytest.fixture
def record():
    identity = NodeIdentity.from_private_key(
        Ed25519PrivateKey.generate()
    )
    return create_alias_record(identity, "alice", 1)


def test_store_request_roundtrip(record):
    data = encode_store_alias_request(record)
    decoded = decode_store_alias_request(data)

    assert decoded["record"] == record
    assert decoded["key"] == alias_record_key("alice")


def test_store_response_roundtrip():
    key = alias_record_key("alice")
    data = encode_store_alias_response(key, True)

    assert decode_store_alias_response(data) == {
        "key": key,
        "stored": True,
    }


def test_find_request_roundtrip():
    data = encode_find_alias_request("alice")
    decoded = decode_find_alias_request(data)

    assert decoded["alias"] == "alice"
    assert decoded["key"] == alias_record_key("alice")


def test_find_response_roundtrip(record):
    data = encode_find_alias_response("alice", [record])
    decoded = decode_find_alias_response(data)

    assert decoded["records"] == [record]


def test_empty_find_response():
    data = encode_find_alias_response("alice", [])

    assert decode_find_alias_response(data)["records"] == []


def test_wrong_alias_rejected(record):
    with pytest.raises(PayloadError):
        encode_find_alias_response("bob", [record])


def test_duplicate_owner_rejected(record):
    with pytest.raises(PayloadError):
        encode_find_alias_response(
            "alice", [record, record]
        )


def test_invalid_alias_rejected():
    with pytest.raises(PayloadError):
        encode_find_alias_request("Alice")


def test_invalid_store_flag():
    with pytest.raises(PayloadError):
        encode_store_alias_response(
            alias_record_key("alice"), 1
        )
