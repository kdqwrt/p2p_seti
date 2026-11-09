
from dataclasses import replace

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
)

from p2p.identity.identity import NodeIdentity
from p2p.dht.alias_records import (
    create_alias_record,
    alias_record_key,
)
from p2p.dht.alias_storage import (
    AliasStorage,
    AliasRollbackError,
    AliasConflictError,
    AliasStorageError,
)


@pytest.fixture
def identity():
    return NodeIdentity.from_private_key(
        Ed25519PrivateKey.generate()
    )


@pytest.fixture
def storage(tmp_path):
    return AliasStorage(tmp_path)


@pytest.mark.asyncio
async def test_put_and_get(storage, identity):
    record = create_alias_record(identity, "alice", 1)
    key = await storage.put(record)

    assert key == alias_record_key("alice")
    assert await storage.get("alice") == record
    assert await storage.count() == 1


@pytest.mark.asyncio
async def test_missing_alias(storage):
    assert await storage.get("missing") is None


@pytest.mark.asyncio
async def test_persistence(tmp_path, identity):
    first = AliasStorage(tmp_path)
    record = create_alias_record(identity, "alice", 1)
    await first.put(record)

    second = AliasStorage(tmp_path)
    assert await second.load() == 1
    assert await second.get("alice") == record


@pytest.mark.asyncio
async def test_sequence_rollback(storage, identity):
    newer = create_alias_record(identity, "alice", 2)
    older = create_alias_record(identity, "alice", 1)

    await storage.put(newer)

    with pytest.raises(AliasRollbackError):
        await storage.put(older)


@pytest.mark.asyncio
async def test_same_sequence_conflict(storage, identity):
    first = create_alias_record(identity, "alice", 1)
    second = create_alias_record(
        identity, "alice", 1, ttl_seconds=120
    )

    await storage.put(first)

    with pytest.raises(AliasConflictError):
        await storage.put(second)


@pytest.mark.asyncio
async def test_idempotent_store(storage, identity):
    record = create_alias_record(identity, "alice", 1)

    await storage.put(record)
    await storage.put(record)

    assert await storage.count() == 1


@pytest.mark.asyncio
async def test_multiple_owners(storage):
    owner_a = NodeIdentity.from_private_key(
        Ed25519PrivateKey.generate()
    )
    owner_b = NodeIdentity.from_private_key(
        Ed25519PrivateKey.generate()
    )

    a = create_alias_record(owner_a, "alice", 1)
    b = create_alias_record(owner_b, "alice", 1)

    await storage.put(a)
    await storage.put(b)

    assert await storage.count() == 2
    assert set(await storage.get_all("alice")) == {a, b}
    assert await storage.get("alice") in (a, b)


@pytest.mark.asyncio
async def test_conflict_order_independent(tmp_path):
    a_identity = NodeIdentity.from_private_key(
        Ed25519PrivateKey.generate()
    )
    b_identity = NodeIdentity.from_private_key(
        Ed25519PrivateKey.generate()
    )

    a = create_alias_record(a_identity, "alice", 1)
    b = create_alias_record(b_identity, "alice", 1)

    first = AliasStorage(tmp_path / "first")
    second = AliasStorage(tmp_path / "second")

    await first.put(a)
    await first.put(b)

    await second.put(b)
    await second.put(a)

    assert await first.get("alice") == await second.get("alice")


@pytest.mark.asyncio
async def test_expiration(storage, identity):
    record = create_alias_record(
        identity, "alice", 1, ttl_seconds=10
    )
    await storage.put(record)

    removed = await storage.cleanup(now=record.expires_at)

    assert removed == 1
    assert await storage.get("alice") is None


@pytest.mark.asyncio
async def test_rollback_after_expiration(storage, identity):
    newer = create_alias_record(identity, "alice", 5)
    await storage.put(newer)

    await storage.cleanup(now=newer.expires_at)

    older = create_alias_record(identity, "alice", 4)

    with pytest.raises(AliasRollbackError):
        await storage.put(older)


@pytest.mark.asyncio
async def test_rollback_after_restart(tmp_path, identity):
    first = AliasStorage(tmp_path)
    newer = create_alias_record(identity, "alice", 5)
    await first.put(newer)

    second = AliasStorage(tmp_path)
    await second.load()

    older = create_alias_record(identity, "alice", 4)

    with pytest.raises(AliasRollbackError):
        await second.put(older)


@pytest.mark.asyncio
async def test_tampered_record_rejected(storage, identity):
    record = create_alias_record(identity, "alice", 1)
    tampered = replace(record, alias="other")

    with pytest.raises(ValueError):
        await storage.put(tampered)


@pytest.mark.asyncio
async def test_corrupted_version_journal(tmp_path, identity):
    storage = AliasStorage(tmp_path)
    record = create_alias_record(identity, "alice", 1)
    await storage.put(record)

    version_file = next(
        (tmp_path / "aliases" / "versions").glob("*.json")
    )
    version_file.write_text("invalid", encoding="utf-8")

    with pytest.raises(AliasStorageError):
        AliasStorage(tmp_path)
