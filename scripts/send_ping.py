import argparse
import asyncio
import secrets
import time

from p2p.protocol.constants import (
    PROTOCOL_VERSION,
    MessageType,
)
from p2p.protocol.contact import Contact
from p2p.protocol.frame import Frame
from p2p.protocol.messages import (
    decode_error,
    decode_pong,
    encode_ping,
)
from p2p.transport.connection import send_request


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send a test PING request to a P2P node."
    )

    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Target node host",
    )

    parser.add_argument(
        "--port",
        type=int,
        required=True,
        help="Target node port",
    )

    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    request_id = secrets.token_bytes(16)
    timestamp_ms = int(time.time() * 1000)

    # Временный Contact этапа 1.
    # Полноценная Ed25519 identity появится на этапе 2.
    sender = Contact(
        node_id=b"",
        identity_algorithm="stage1-none",
        identity_public_key=b"",
        host="127.0.0.1",
        port=65535,
    )

    payload = encode_ping(
        sender=sender,
        timestamp_ms=timestamp_ms,
    )

    frame = Frame(
        version=PROTOCOL_VERSION,
        message_type=MessageType.PING,
        flags=0,
        request_id=request_id,
        payload=payload,
    )

    print(
        f"Sending PING request_id={request_id.hex()}"
    )

    response = await send_request(
        host=args.host,
        port=args.port,
        frame=frame,
    )

    print(
        f"Received {response.message_type.name}"
    )
    print(
        f"request_id={response.request_id.hex()}"
    )

    if response.message_type == MessageType.PONG:
        pong = decode_pong(response.payload)

        if pong["ping_timestamp_ms"] != timestamp_ms:
            raise RuntimeError(
                "PONG contains unexpected ping_timestamp_ms"
            )

        print(
            "PONG payload:"
        )
        print(
            f"  responder={pong['responder']}"
        )
        print(
            f"  ping_timestamp_ms="
            f"{pong['ping_timestamp_ms']}"
        )
        print(
            f"  responder_timestamp_ms="
            f"{pong['responder_timestamp_ms']}"
        )

    elif response.message_type == MessageType.ERROR:
        error = decode_error(response.payload)

        print(
            f"ERROR code={error['code']}"
        )
        print(
            f"ERROR message={error['message']}"
        )

    else:
        raise RuntimeError(
            "Unexpected response type: "
            f"{response.message_type.name}"
        )


if __name__ == "__main__":
    asyncio.run(main())