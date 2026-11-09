
import pytest

from p2p.dht.maintenance import ReplicaMaintenance
from p2p.dht.records import (
    create_node_record,
    node_record_key,
)
from p2p.identity.identity import (
    IDENTITY_ALGORITHM,
    load_or_create_identity,
)
from p2p.protocol.contact import Contact
from p2p.routing.distance import xor_distance


@pytest.fixture
def maintenance_data(tmp_path):
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
            contact.node_id,
            key,
        )
    )

    return record, contacts


@pytest.mark.asyncio
async def test_all_replicas_already_exist(maintenance_data):
    record, contacts = maintenance_data

    stored = {
        contact.node_id
        for contact in contacts[:3]
    }

    store_calls = []

    async def find_value(contact, key):
        return record if contact.node_id in stored else None

    async def store(contact, record):
        store_calls.append(contact.node_id)
        return True

    maintenance = ReplicaMaintenance(
        find_value=find_value,
        store=store,
    )

    result = await maintenance.maintain(
        record,
        contacts,
    )

    assert result.existing == 3
    assert result.repaired == 0
    assert result.confirmed == 3
    assert result.fully_replicated is True
    assert store_calls == []


@pytest.mark.asyncio
async def test_one_missing_replica_is_repaired(maintenance_data):
    record, contacts = maintenance_data

    stored = {
        contacts[0].node_id,
        contacts[1].node_id,
    }

    async def find_value(contact, key):
        return record if contact.node_id in stored else None

    async def store(contact, record):
        stored.add(contact.node_id)
        return True

    maintenance = ReplicaMaintenance(
        find_value=find_value,
        store=store,
    )

    result = await maintenance.maintain(
        record,
        contacts,
    )

    assert result.existing == 2
    assert result.repaired == 1
    assert result.confirmed == 3
    assert result.successful is True
    assert result.fully_replicated is True


@pytest.mark.asyncio
async def test_two_missing_replicas_are_repaired(
    maintenance_data,
):
    record, contacts = maintenance_data

    stored = {contacts[0].node_id}

    async def find_value(contact, key):
        return record if contact.node_id in stored else None

    async def store(contact, record):
        stored.add(contact.node_id)
        return True

    maintenance = ReplicaMaintenance(
        find_value=find_value,
        store=store,
    )

    result = await maintenance.maintain(
        record,
        contacts,
    )

    assert result.existing == 1
    assert result.repaired == 2
    assert result.confirmed == 3
    assert result.fully_replicated is True


@pytest.mark.asyncio
async def test_repair_uses_reserve_after_failure(
    maintenance_data,
):
    record, contacts = maintenance_data

    stored = {
        contacts[0].node_id,
        contacts[1].node_id,
    }

    failed_id = contacts[2].node_id

    async def find_value(contact, key):
        return record if contact.node_id in stored else None

    async def store(contact, record):
        if contact.node_id == failed_id:
            raise ConnectionError("Node unavailable")

        stored.add(contact.node_id)
        return True

    maintenance = ReplicaMaintenance(
        find_value=find_value,
        store=store,
    )

    result = await maintenance.maintain(
        record,
        contacts,
    )

    assert result.existing == 2
    assert result.repaired == 1
    assert result.confirmed == 3
    assert result.fully_replicated is True
    assert contacts[3].node_id in stored


@pytest.mark.asyncio
async def test_quorum_without_full_replication(
    maintenance_data,
):
    record, contacts = maintenance_data

    stored = {
        contacts[0].node_id,
        contacts[1].node_id,
    }

    async def find_value(contact, key):
        return record if contact.node_id in stored else None

    async def store(contact, record):
        return False

    maintenance = ReplicaMaintenance(
        find_value=find_value,
        store=store,
    )

    result = await maintenance.maintain(
        record,
        contacts,
    )

    assert result.existing == 2
    assert result.repaired == 0
    assert result.confirmed == 2
    assert result.successful is True
    assert result.fully_replicated is False


@pytest.mark.asyncio
async def test_failure_when_quorum_is_lost(
    maintenance_data,
):
    record, contacts = maintenance_data

    stored = {contacts[0].node_id}

    async def find_value(contact, key):
        return record if contact.node_id in stored else None

    async def store(contact, record):
        return False

    maintenance = ReplicaMaintenance(
        find_value=find_value,
        store=store,
    )

    result = await maintenance.maintain(
        record,
        contacts,
    )

    assert result.existing == 1
    assert result.repaired == 0
    assert result.confirmed == 1
    assert result.successful is False


@pytest.mark.asyncio
async def test_duplicate_contacts_are_ignored(
    maintenance_data,
):
    record, contacts = maintenance_data

    checked_ids = []

    async def find_value(contact, key):
        checked_ids.append(contact.node_id)
        return record

    async def store(contact, record):
        return True

    maintenance = ReplicaMaintenance(
        find_value=find_value,
        store=store,
    )

    result = await maintenance.maintain(
        record,
        contacts + contacts,
    )

    assert result.checked == 5
    assert len(checked_ids) == 5
    assert len(set(checked_ids)) == 5


def test_invalid_replication_configuration():
    async def find_value(contact, key):
        return None

    async def store(contact, record):
        return True

    with pytest.raises(ValueError):
        ReplicaMaintenance(
            find_value=find_value,
            store=store,
            replication_factor=0,
        )

    with pytest.raises(ValueError):
        ReplicaMaintenance(
            find_value=find_value,
            store=store,
            replication_factor=3,
            required_confirmations=4,
        )



@pytest.mark.asyncio
async def test_newer_remote_record_is_not_overwritten(tmp_path):
    identity = load_or_create_identity(
        str(tmp_path / "owner")
    )

    record = create_node_record(
        identity=identity,
        addresses=["127.0.0.1:9101"],
        sequence_number=1,
    )

    newer = create_node_record(
        identity=identity,
        addresses=["127.0.0.1:9999"],
        sequence_number=2,
    )

    peer_identity = load_or_create_identity(
        str(tmp_path / "peer")
    )

    contact = Contact(
        node_id=peer_identity.node_id,
        identity_algorithm=IDENTITY_ALGORITHM,
        identity_public_key=peer_identity.public_key,
        host="127.0.0.1",
        port=9201,
    )

    store_calls = []

    async def find_value(peer, key):
        return newer

    async def store(peer, value):
        store_calls.append(peer.node_id)
        return True

    maintenance = ReplicaMaintenance(
        find_value=find_value,
        store=store,
    )

    result = await maintenance.maintain(
        record,
        [contact],
    )

    assert store_calls == []
    assert result.confirmed == 0
    assert result.successful is False

