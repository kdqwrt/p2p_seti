import pytest

from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
)
from cryptography.hazmat.primitives import serialization

from p2p.identity.identity import (
    IDENTITY_ALGORITHM,
    calculate_node_id,
)
from p2p.protocol.contact import Contact
from p2p.routing.lookup import iterative_lookup


def make_contact(port: int) -> Contact:
    """Создаёт контакт с корректной идентичностью Ed25519."""

    private_key = Ed25519PrivateKey.generate()

    public_key = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )

    return Contact(
        node_id=calculate_node_id(public_key),
        identity_algorithm=IDENTITY_ALGORITHM,
        identity_public_key=public_key,
        host="127.0.0.1",
        port=port,
    )


@pytest.mark.asyncio
async def test_lookup_continues_after_verification_failure():
    """
    Проверяет обнаружение недоступного контакта
    и продолжение итеративного поиска.
    """

    local = make_contact(9101)
    peer = make_contact(9102)
    failed = make_contact(9103)
    target = make_contact(9104)

    async def fake_find_node(contact, requested_target):
        assert requested_target == target.node_id

        if contact.node_id == peer.node_id:
            return [failed, target]

        return []

    async def fake_verify_contact(contact):
        # Узел failed имитирует недоступный узел.
        return contact.node_id != failed.node_id

    result = await iterative_lookup(
        local_node_id=local.node_id,
        target_node_id=target.node_id,
        initial_contacts=[peer],
        k=4,
        alpha=3,
        find_node=fake_find_node,
        verify_contact=fake_verify_contact,
        collect_trace=True,
    )

    # Целевой узел найден.
    assert result.found is True

    # Проверены два обнаруженных контакта.
    assert result.verification_count == 2

    # Одна проверка завершилась неудачно.
    assert result.verification_failures == 1

    # Недоступный контакт зафиксирован.
    assert failed.node_id.hex() in result.failed_contacts

    # Цель не признана недоступной.
    assert target.node_id.hex() not in result.failed_contacts

    # Время проверок учитывается.
    assert result.verification_duration_ms >= 0

    # Поиск продолжился после проверок.
    assert result.rpc_count >= 2

    # Трассировка сформирована.
    assert result.trace is not None

    # Недоступному узлу FIND_NODE не отправлялся.
    assert all(
        event.peer_node_id != failed.node_id.hex()
        for event in result.trace
    )