
import asyncio

import pytest

from p2p.dht.records import create_node_record
from p2p.dht.replication import replicate_record
from p2p.identity.identity import load_or_create_identity
from p2p.protocol.contact import Contact
from p2p.identity.identity import IDENTITY_ALGORITHM


@pytest.fixture
def replication_data(tmp_path):
    owner = load_or_create_identity(
        str(tmp_path / "owner")
    )

    record = create_node_record(
        identity=owner,
        addresses=["127.0.0.1:9101"],
        sequence_number=1,
    )

    contacts = []

    for index in range(3):
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

    return record, contacts


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "success_count, expected",
    [
        (3, True),
        (2, True),
        (1, False),
        (0, False),
    ],
)
async def test_replication_quorum(
    replication_data,
    success_count,
    expected,
):
    record, contacts = replication_data

    successful_ids = {
        contact.node_id
        for contact in contacts[:success_count]
    }

    async def fake_store(contact, record):
        await asyncio.sleep(0)
        return contact.node_id in successful_ids

    result = await replicate_record(
        record=record,
        contacts=contacts,
        store=fake_store,
    )

    assert result.attempted == 3
    assert result.confirmed == success_count
    assert result.required == 2
    assert result.successful is expected


@pytest.mark.asyncio
async def test_replication_one_unavailable_node(
    replication_data,
):
    record, contacts = replication_data

    async def fake_store(contact, record):
        if contact.node_id == contacts[0].node_id:
            raise ConnectionError("Node unavailable")

        return True

    result = await replicate_record(
        record=record,
        contacts=contacts,
        store=fake_store,
    )

    assert result.attempted == 3
    assert result.confirmed == 2
    assert result.successful is True
    assert len(result.failed_nodes) == 1


@pytest.mark.asyncio
async def test_replication_deduplicates_contacts(
    replication_data,
):
    record, contacts = replication_data

    calls = []

    async def fake_store(contact, record):
        calls.append(contact.node_id)
        return True

    result = await replicate_record(
        record=record,
        contacts=contacts + [contacts[0]],
        store=fake_store,
    )

    assert result.attempted == 3
    assert len(calls) == 3
    assert len(set(calls)) == 3
