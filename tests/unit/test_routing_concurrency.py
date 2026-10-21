
import asyncio

import pytest

from p2p.identity.identity import load_or_create_identity
from p2p.protocol.contact import Contact
from p2p.routing.distance import bucket_index
from p2p.routing.routing_table import RoutingTable


def make_contact(identity, port):
    return Contact(
        node_id=identity.node_id,
        identity_algorithm="ed25519",
        identity_public_key=identity.public_key,
        host="127.0.0.1",
        port=port,
    )


def make_same_bucket_contacts(tmp_path, local_id, count):
    """Создаёт контакты, принадлежащие одному bucket."""
    groups = {}

    for index in range(100):
        identity = load_or_create_identity(
            tmp_path / f"remote_{index}"
        )
        contact = make_contact(identity, 10000 + index)

        index_bucket = bucket_index(
            local_id,
            contact.node_id,
        )

        group = groups.setdefault(index_bucket, [])
        group.append(contact)

        if len(group) >= count:
            return group[:count]

    raise AssertionError("Could not find enough contacts")


@pytest.mark.asyncio
async def test_unverified_contact_is_not_added(tmp_path):
    local = load_or_create_identity(tmp_path / "local")
    remote = load_or_create_identity(tmp_path / "remote")

    table = RoutingTable(local.node_id, k_bucket_size=4)

    async def ping_callback(_):
        return True

    result = await table.add_contact(
        make_contact(remote, 9101),
        ping_callback,
        verified=False,
    )

    assert result is False
    assert table.size() == 0


@pytest.mark.asyncio
async def test_alive_lru_moves_to_end(tmp_path):
    local = load_or_create_identity(tmp_path / "local")

    table = RoutingTable(local.node_id, k_bucket_size=2)

    first, second, third = make_same_bucket_contacts(
        tmp_path,
        local.node_id,
        3,
    )

    async def ping_alive(_):
        return True

    await table.add_contact(first, ping_alive)
    await table.add_contact(second, ping_alive)

    bucket = table.get_bucket(first.node_id)

    assert bucket.oldest().node_id == first.node_id

    result = await table.add_contact(third, ping_alive)

    assert result is False
    assert bucket.oldest().node_id == second.node_id
    assert bucket.contacts[-1].node_id == first.node_id
    assert table.get_contact(third.node_id) is None


@pytest.mark.asyncio
async def test_concurrent_additions_do_not_overfill_bucket(tmp_path):
    local = load_or_create_identity(tmp_path / "local")

    table = RoutingTable(local.node_id, k_bucket_size=2)

    contacts = make_same_bucket_contacts(
        tmp_path,
        local.node_id,
        8,
    )

    ping_calls = 0

    async def ping_alive(_):
        nonlocal ping_calls
        ping_calls += 1
        await asyncio.sleep(0.01)
        return True

    await asyncio.gather(
        *[
            table.add_contact(contact, ping_alive)
            for contact in contacts
        ]
    )

    bucket = table.get_bucket(contacts[0].node_id)

    assert len(bucket.contacts) == 2
    assert table.size() == 2
    assert ping_calls == 6


@pytest.mark.asyncio
async def test_concurrent_replacement_keeps_capacity(tmp_path):
    local = load_or_create_identity(tmp_path / "local")

    table = RoutingTable(local.node_id, k_bucket_size=2)

    contacts = make_same_bucket_contacts(
        tmp_path,
        local.node_id,
        6,
    )

    async def ping_dead(_):
        await asyncio.sleep(0.01)
        return False

    await asyncio.gather(
        *[
            table.add_contact(contact, ping_dead)
            for contact in contacts
        ]
    )

    bucket = table.get_bucket(contacts[0].node_id)

    assert len(bucket.contacts) == 2
    assert table.size() == 2
    assert len({
        contact.node_id
        for contact in bucket.contacts
    }) == 2
