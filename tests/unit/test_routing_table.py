
import pytest

from p2p.identity.identity import load_or_create_identity
from p2p.protocol.contact import Contact
from p2p.routing.routing_table import RoutingTable


def make_contact(identity, port):
    return Contact(
        node_id=identity.node_id,
        identity_algorithm="ed25519",
        identity_public_key=identity.public_key,
        host="127.0.0.1",
        port=port,
    )


def make_contacts(tmp_path, count):
    contacts = []

    for index in range(count):
        identity = load_or_create_identity(
            tmp_path / f"node{index}"
        )
        contacts.append(make_contact(identity, 9100 + index))

    return contacts


@pytest.mark.asyncio
async def test_add_contact_to_empty_bucket(tmp_path):
    local = load_or_create_identity(tmp_path / "local")
    remote = load_or_create_identity(tmp_path / "remote")

    table = RoutingTable(local.node_id, k_bucket_size=4)
    contact = make_contact(remote, 9101)

    async def ping_callback(_):
        return True

    assert await table.add_contact(contact, ping_callback)
    assert table.get_contact(remote.node_id) == contact
    assert table.size() == 1


@pytest.mark.asyncio
async def test_local_node_is_not_added(tmp_path):
    local = load_or_create_identity(tmp_path / "local")
    table = RoutingTable(local.node_id, k_bucket_size=4)

    async def ping_callback(_):
        return True

    result = await table.add_contact(
        make_contact(local, 9101),
        ping_callback,
    )

    assert result is False
    assert table.size() == 0


@pytest.mark.asyncio
async def test_duplicate_contact_is_not_added_twice(tmp_path):
    local = load_or_create_identity(tmp_path / "local")
    remote = load_or_create_identity(tmp_path / "remote")

    table = RoutingTable(local.node_id, k_bucket_size=4)
    contact = make_contact(remote, 9101)

    async def ping_callback(_):
        return True

    await table.add_contact(contact, ping_callback)
    await table.add_contact(contact, ping_callback)

    assert table.size() == 1


@pytest.mark.asyncio
async def test_bucket_capacity_and_alive_lru(tmp_path):
    local = load_or_create_identity(tmp_path / "local")
    table = RoutingTable(local.node_id, k_bucket_size=2)

    contacts = make_contacts(tmp_path, 20)

    groups = {}

    for contact in contacts:
        index = (
            int.from_bytes(
                local.node_id, "big"
            ) ^ int.from_bytes(
                contact.node_id, "big"
            )
        ).bit_length() - 1

        groups.setdefault(index, []).append(contact)

    selected = next(
        group for group in groups.values()
        if len(group) >= 3
    )

    async def ping_alive(_):
        return True

    await table.add_contact(selected[0], ping_alive)
    await table.add_contact(selected[1], ping_alive)

    result = await table.add_contact(
        selected[2], ping_alive
    )

    assert result is False
    assert table.size() == 2
    assert table.get_contact(selected[0].node_id) is not None
    assert table.get_contact(selected[2].node_id) is None


@pytest.mark.asyncio
async def test_dead_lru_is_replaced(tmp_path):
    local = load_or_create_identity(tmp_path / "local")
    table = RoutingTable(local.node_id, k_bucket_size=2)

    contacts = make_contacts(tmp_path, 20)

    groups = {}

    for contact in contacts:
        index = (
            int.from_bytes(
                local.node_id, "big"
            ) ^ int.from_bytes(
                contact.node_id, "big"
            )
        ).bit_length() - 1

        groups.setdefault(index, []).append(contact)

    selected = next(
        group for group in groups.values()
        if len(group) >= 3
    )

    async def ping_dead(_):
        return False

    await table.add_contact(selected[0], ping_dead)
    await table.add_contact(selected[1], ping_dead)

    result = await table.add_contact(
        selected[2], ping_dead
    )

    assert result is True
    assert table.size() == 2
    assert table.get_contact(selected[0].node_id) is None
    assert table.get_contact(selected[2].node_id) is not None


@pytest.mark.asyncio
async def test_closest_contacts_are_sorted(tmp_path):
    local = load_or_create_identity(tmp_path / "local")
    table = RoutingTable(local.node_id, k_bucket_size=4)

    contacts = make_contacts(tmp_path, 10)

    async def ping_callback(_):
        return True

    for contact in contacts:
        await table.add_contact(contact, ping_callback)

    target = contacts[0].node_id
    result = table.closest(target, limit=4)

    distances = [
        int.from_bytes(
            contact.node_id, "big"
        ) ^ int.from_bytes(target, "big")
        for contact in result
    ]

    assert distances == sorted(distances)
    assert len(result) <= 4
