
import pytest

from p2p.dht.records import create_node_record
from p2p.dht.replication import replicate_record
from p2p.identity.identity import (
    IDENTITY_ALGORITHM,
    load_or_create_identity,
)
from p2p.protocol.contact import Contact
from p2p.routing.distance import xor_distance
from p2p.dht.records import node_record_key


@pytest.fixture
def data(tmp_path):
    owner = load_or_create_identity(
        str(tmp_path / "owner")
    )

    record = create_node_record(
        identity=owner,
        addresses=["127.0.0.1:9101"],
        sequence_number=1,
    )

    contacts = []

    for index in range(5):
        identity = load_or_create_identity(
            str(tmp_path / f"peer{index}")
        )

        contacts.append(
            Contact(
                node_id=identity.node_id,
                identity_algorithm=IDENTITY_ALGORITHM,
                identity_public_key=identity.public_key,
                host="127.0.0.1",
                port=9201 + index,
            )
        )

    key = node_record_key(record.node_id)

    contacts.sort(
        key=lambda contact: xor_distance(
            contact.node_id, key
        )
    )

    return record, contacts


@pytest.mark.asyncio
async def test_fallback_replaces_one_failed_replica(data):
    record, contacts = data

    failed_id = contacts[0].node_id

    async def fake_store(contact, record):
        if contact.node_id == failed_id:
            raise ConnectionError("Node offline")
        return True

    result = await replicate_record(
    record=record,
    contacts=contacts,
    store=fake_store,
    )

    assert result.attempted == 4
    assert result.confirmed == 3
    assert result.successful is True
    assert result.fully_replicated is True
    assert failed_id in result.failed_nodes
    assert contacts[3].node_id in result.confirmed_nodes


@pytest.mark.asyncio
async def test_fallback_replaces_two_failed_replicas(data):
    record, contacts = data

    failed_ids = {
        contacts[0].node_id,
        contacts[1].node_id,
    }

    async def fake_store(contact, record):
        return contact.node_id not in failed_ids

    result = await replicate_record(
        record=record,
        contacts=contacts,
        store=fake_store,
    )

    assert result.attempted == 5
    assert result.confirmed == 3
    assert result.successful is True
    assert result.fully_replicated is True


@pytest.mark.asyncio
async def test_quorum_without_full_replication(data):
    record, contacts = data

    successful_ids = {
        contacts[0].node_id,
        contacts[1].node_id,
    }

    async def fake_store(contact, record):
        return contact.node_id in successful_ids

    result = await replicate_record(
        record=record,
        contacts=contacts,
        store=fake_store,
    )

    assert result.attempted == 5
    assert result.confirmed == 2
    assert result.successful is True
    assert result.fully_replicated is False


@pytest.mark.asyncio
async def test_failure_when_quorum_not_reached(data):
    record, contacts = data

    async def fake_store(contact, record):
        return contact.node_id == contacts[0].node_id

    result = await replicate_record(
        record=record,
        contacts=contacts,
        store=fake_store,
    )

    assert result.attempted == 5
    assert result.confirmed == 1
    assert result.successful is False
    assert result.fully_replicated is False
