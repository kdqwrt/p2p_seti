
import secrets

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
)

from p2p.identity.identity import NodeIdentity
from p2p.protocol.contact import Contact
from p2p.protocol.constants import MessageType
from p2p.protocol.frame import Frame
from p2p.transport.connection import send_request
from p2p.transport.server import TCPServer
from p2p.dht.alias_records import create_alias_record
from p2p.dht.alias_storage import AliasStorage
from p2p.dht.alias_messages import (
    encode_store_alias_request,
    decode_store_alias_response,
    encode_find_alias_request,
    decode_find_alias_response,
)


@pytest.fixture
def identity():
    return NodeIdentity.from_private_key(
        Ed25519PrivateKey.generate()
    )


@pytest.fixture
def contact(identity):
    return Contact(
        node_id=identity.node_id,
        identity_algorithm="ed25519",
        identity_public_key=identity.public_key,
        host="127.0.0.1",
        port=9102,
    )


async def rpc(port, message_type, payload):
    request = Frame(
        version=1,
        message_type=message_type,
        flags=0,
        request_id=secrets.token_bytes(16),
        payload=payload,
    )

    response = await send_request(
        host="127.0.0.1",
        port=port,
        frame=request,
        connect_timeout=2.0,
        response_timeout=2.0,
        protocol_version=1,
        max_frame_payload=65536,
        local_node_label="alias-test",
    )

    assert response.request_id == request.request_id
    return response


@pytest.mark.asyncio
async def test_store_and_find_alias_over_tcp(
    tmp_path, identity, contact
):
    storage = AliasStorage(tmp_path)

    server = TCPServer(
        host="127.0.0.1",
        port=0,
        local_contact=contact,
        read_timeout=2.0,
        protocol_version=1,
        max_frame_payload=65536,
        on_store_alias=storage.put,
        on_find_alias=storage.get_all,
    )

    await server.start()

    try:
        port = server._server.sockets[0].getsockname()[1]

        record = create_alias_record(
            identity=identity,
            alias="alice",
            sequence_number=1,
        )

        response = await rpc(
            port,
            MessageType.STORE_ALIAS_REQUEST,
            encode_store_alias_request(record),
        )

        assert response.message_type == MessageType.STORE_ALIAS_RESPONSE

        decoded = decode_store_alias_response(response.payload)
        assert decoded["stored"] is True

        response = await rpc(
            port,
            MessageType.FIND_ALIAS_REQUEST,
            encode_find_alias_request("alice"),
        )

        assert response.message_type == MessageType.FIND_ALIAS_RESPONSE

        decoded = decode_find_alias_response(response.payload)
        assert decoded["records"] == [record]

    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_find_missing_alias_over_tcp(tmp_path, contact):
    storage = AliasStorage(tmp_path)

    server = TCPServer(
        host="127.0.0.1",
        port=0,
        local_contact=contact,
        read_timeout=2.0,
        protocol_version=1,
        max_frame_payload=65536,
        on_store_alias=storage.put,
        on_find_alias=storage.get_all,
    )

    await server.start()

    try:
        port = server._server.sockets[0].getsockname()[1]

        response = await rpc(
            port,
            MessageType.FIND_ALIAS_REQUEST,
            encode_find_alias_request("missing"),
        )

        assert response.message_type == MessageType.FIND_ALIAS_RESPONSE

        decoded = decode_find_alias_response(response.payload)
        assert decoded["records"] == []

    finally:
        await server.stop()
