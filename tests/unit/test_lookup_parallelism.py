
import asyncio

import pytest

from p2p.routing.lookup import iterative_lookup


@pytest.mark.asyncio
async def test_lookup_respects_alpha(tmp_path):
    """Число одновременных FIND_NODE не превышает ALPHA."""

    alpha = 3
    k = 8

    local_node_id = bytes(32)
    target_node_id = bytes([255]) * 32

    active_requests = 0
    max_active_requests = 0

    from p2p.identity.identity import load_or_create_identity
    from p2p.protocol.contact import Contact
    from p2p.identity.identity import IDENTITY_ALGORITHM

    contacts = []

    for i in range(8):
        identity = load_or_create_identity(
            tmp_path / f"peer_{i}"
        )

        contacts.append(
            Contact(
                node_id=identity.node_id,
                identity_algorithm=IDENTITY_ALGORITHM,
                identity_public_key=identity.public_key,
                host="127.0.0.1",
                port=12000 + i,
            )
        )

    async def fake_find_node(contact, target):
        nonlocal active_requests, max_active_requests

        active_requests += 1
        max_active_requests = max(
            max_active_requests,
            active_requests,
        )

        try:
            await asyncio.sleep(0.02)
            return []
        finally:
            active_requests -= 1

    async def fake_verify(contact):
        return True

    result = await iterative_lookup(
        local_node_id=local_node_id,
        target_node_id=target_node_id,
        initial_contacts=contacts,
        k=k,
        alpha=alpha,
        find_node=fake_find_node,
        verify_contact=fake_verify,
    )

    assert max_active_requests <= alpha
    assert max_active_requests == alpha
    assert result.rpc_count == 8
    assert result.iterations == 3
