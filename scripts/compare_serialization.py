"""Воспроизводимое сравнение JSON и MessagePack для E1-8."""

from __future__ import annotations

import json
import statistics
import timeit

import msgpack

ITERATIONS = 20_000
REPEATS = 7

# Один и тот же логический PING. В JSON bytes представлены hex-строкой.
JSON_PAYLOAD = {
    "sender": {
        "node_id": "",
        "identity_algorithm": "stage1-none",
        "identity_public_key": "",
        "host": "127.0.0.1",
        "port": 9101,
    },
    "timestamp_ms": 1_797_000_000_000,
}

MSGPACK_PAYLOAD = {
    "sender": {
        "node_id": b"",
        "identity_algorithm": "stage1-none",
        "identity_public_key": b"",
        "host": "127.0.0.1",
        "port": 9101,
    },
    "timestamp_ms": 1_797_000_000_000,
}


def encode_json() -> bytes:
    return json.dumps(
        JSON_PAYLOAD,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def decode_json(data: bytes) -> dict:
    return json.loads(data.decode("utf-8"))


def encode_msgpack() -> bytes:
    return msgpack.packb(MSGPACK_PAYLOAD, use_bin_type=True)


def decode_msgpack(data: bytes) -> dict:
    return msgpack.unpackb(data, raw=False)


def median_ns_per_operation(stmt: str, globals_dict: dict) -> float:
    samples = timeit.repeat(
        stmt,
        globals=globals_dict,
        number=ITERATIONS,
        repeat=REPEATS,
    )
    return statistics.median(samples) * 1_000_000_000 / ITERATIONS


def main() -> None:
    json_bytes = encode_json()
    msgpack_bytes = encode_msgpack()

    benchmark_globals = {**globals(), "json_bytes": json_bytes, "msgpack_bytes": msgpack_bytes}
    json_encode = median_ns_per_operation("encode_json()", benchmark_globals)
    json_decode = median_ns_per_operation("decode_json(json_bytes)", benchmark_globals)
    msgpack_encode = median_ns_per_operation("encode_msgpack()", benchmark_globals)
    msgpack_decode = median_ns_per_operation("decode_msgpack(msgpack_bytes)", benchmark_globals)

    print("Stage 1 serialization comparison")
    print(f"Iterations per repeat: {ITERATIONS}; repeats: {REPEATS}")
    print()
    print(f"{'Format':<12} {'Size, B':>10} {'Encode, ns/op':>16} {'Decode, ns/op':>16}")
    print("-" * 58)
    print(f"{'JSON':<12} {len(json_bytes):>10} {json_encode:>16.1f} {json_decode:>16.1f}")
    print(f"{'MessagePack':<12} {len(msgpack_bytes):>10} {msgpack_encode:>16.1f} {msgpack_decode:>16.1f}")
    print()
    print("Timing depends on the machine and Python build; rerun this script for reproducible local measurements.")


if __name__ == "__main__":
    main()
