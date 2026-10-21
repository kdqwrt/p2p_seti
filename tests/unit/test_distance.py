
import pytest

from p2p.routing.distance import (
    bucket_index,
    xor_distance,
)


def make_id(value: int) -> bytes:
    return value.to_bytes(32, "big")


def test_xor_distance():
    assert xor_distance(make_id(10), make_id(12)) == 6


def test_distance_is_symmetric():
    first = make_id(123)
    second = make_id(456)

    assert xor_distance(first, second) == xor_distance(
        second, first
    )


def test_distance_to_self_is_zero():
    node_id = make_id(12345)

    assert xor_distance(node_id, node_id) == 0


def test_bucket_index():
    local_id = make_id(0)

    assert bucket_index(local_id, make_id(1)) == 0
    assert bucket_index(local_id, make_id(2)) == 1
    assert bucket_index(local_id, make_id(4)) == 2
    assert bucket_index(local_id, make_id(255)) == 7
    assert bucket_index(local_id, make_id(256)) == 8


def test_highest_bucket():
    assert bucket_index(
        make_id(0),
        make_id(1 << 255),
    ) == 255


def test_same_node_has_no_bucket():
    node_id = make_id(42)

    with pytest.raises(ValueError):
        bucket_index(node_id, node_id)


def test_invalid_node_id_length():
    with pytest.raises(ValueError):
        xor_distance(b"short", make_id(1))
