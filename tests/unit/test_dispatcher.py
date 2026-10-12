import secrets

import pytest

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
from p2p.rpc.dispatcher import Dispatcher


@pytest.fixture
def local_contact() -> Contact:
    return Contact(
        node_id=b"",
        identity_algorithm="stage1-none",
        identity_public_key=b"",
        host="127.0.0.1",
        port=9101,
    )


@pytest.fixture
def remote_contact() -> Contact:
    return Contact(
        node_id=b"",
        identity_algorithm="stage1-none",
        identity_public_key=b"",
        host="127.0.0.1",
        port=9102,
    )


@pytest.mark.asyncio
async def test_ping_returns_pong_with_same_request_id(
    local_contact,
    remote_contact,
):
    dispatcher = Dispatcher(local_contact)

    request_id = secrets.token_bytes(16)

    ping = Frame(
        version=PROTOCOL_VERSION,
        message_type=MessageType.PING,
        flags=0,
        request_id=request_id,
        payload=encode_ping(
            sender=remote_contact,
            timestamp_ms=123456789,
        ),
    )

    response = await dispatcher.dispatch(ping)

    assert response.message_type == MessageType.PONG
    assert response.request_id == request_id

    pong = decode_pong(response.payload)

    assert pong["responder"] == local_contact
    assert pong["ping_timestamp_ms"] == 123456789


@pytest.mark.asyncio
async def test_pong_uses_request_protocol_version(
    local_contact,
    remote_contact,
):
    dispatcher = Dispatcher(local_contact)

    ping = Frame(
        version=2,
        message_type=MessageType.PING,
        flags=0,
        request_id=secrets.token_bytes(16),
        payload=encode_ping(
            sender=remote_contact,
            timestamp_ms=123,
        ),
    )

    response = await dispatcher.dispatch(ping)

    assert response.version == 2


@pytest.mark.asyncio
async def test_response_message_as_request_returns_error(
    local_contact,
):
    dispatcher = Dispatcher(local_contact)

    request_id = secrets.token_bytes(16)

    frame = Frame(
        version=PROTOCOL_VERSION,
        message_type=MessageType.PONG,
        flags=0,
        request_id=request_id,
        payload=b"",
    )

    response = await dispatcher.dispatch(frame)

    assert response.message_type == MessageType.ERROR
    assert response.request_id == request_id

    error = decode_error(response.payload)

    assert error["code"] == "INVALID_DIRECTION"


@pytest.mark.asyncio
async def test_find_node_is_not_implemented_in_stage1(
    local_contact,
):
    dispatcher = Dispatcher(local_contact)

    frame = Frame(
        version=PROTOCOL_VERSION,
        message_type=MessageType.FIND_NODE_REQUEST,
        flags=0,
        request_id=secrets.token_bytes(16),
        payload=b"",
    )

    response = await dispatcher.dispatch(frame)

    assert response.message_type == MessageType.ERROR

    error = decode_error(response.payload)

    assert error["code"] == "NOT_IMPLEMENTED"