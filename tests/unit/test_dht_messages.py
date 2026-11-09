
import time

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
)

from p2p.dht.messages import (
    decode_find_value_request,
    decode_find_value_response,
    decode_store_request,
    decode_store_response,
    encode_find_value_request,
    encode_find_value_response,
    encode_store_request,
    encode_store_response,
)
from p2p.dht.records import (
    create_node_record,
    node_record_key,
)
from p2p.identity.identity import NodeIdentity
from p2p.protocol.payload_codec import PayloadError


@pytest.fixture
def sample_record():
    identity = NodeIdentity.from_private_key(
        Ed25519PrivateKey.generate()
    )

    record = create_node_record(
        identity,
        addresses=["127.0.0.1:9101"],
        sequence_number=1,
        now=int(time.time()),
    )

    return node_record_key(identity.node_id), record


def test_store_request_roundtrip(sample_record):
    key, record = sample_record

    encoded = encode_store_request(key, record)
    decoded = decode_store_request(encoded)

    assert decoded["key"] == key
    assert decoded["record"] == record


def test_store_response_roundtrip(sample_record):
    key, _ = sample_record

    encoded = encode_store_response(key, True)
    decoded = decode_store_response(encoded)

    assert decoded == {
        "key": key,
        "stored": True,
    }


def test_find_value_request_roundtrip(sample_record):
    key, _ = sample_record

    encoded = encode_find_value_request(key)
    decoded = decode_find_value_request(encoded)

    assert decoded["key"] == key


def test_find_value_response_found(sample_record):
    key, record = sample_record

    encoded = encode_find_value_response(key, record)
    decoded = decode_find_value_response(encoded)

    assert decoded["found"] is True
    assert decoded["record"] == record


def test_find_value_response_missing(sample_record):
    key, _ = sample_record

    encoded = encode_find_value_response(key, None)
    decoded = decode_find_value_response(encoded)

    assert decoded["found"] is False
    assert decoded["record"] is None


def test_invalid_dht_key_rejected():
    with pytest.raises(PayloadError):
        encode_find_value_request(b"short")


def test_invalid_store_response_flag(sample_record):
    key, _ = sample_record

    with pytest.raises(PayloadError):
        encode_store_response(key, 1)
