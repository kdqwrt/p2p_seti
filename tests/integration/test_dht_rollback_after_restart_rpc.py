
import time

import pytest

from p2p.node.node import Node
from p2p.dht.records import create_node_record, node_record_key
from p2p.dht.storage import RecordRollbackError

from test_dht_store_error_codes_rpc import free_port, make_config


@pytest.mark.asyncio
async def test_rollback_rejected_after_expiration_and_restart(tmp_path):
    ports = []

    while len(ports) < 2:
        port = free_port()
        if port not in ports:
            ports.append(port)

    publisher = Node(make_config(tmp_path / "publisher", ports[0]))
    replica_config = make_config(tmp_path / "replica", ports[1])
    replica = Node(replica_config)

    started = []

    try:
        await publisher.start()
        started.append(publisher)

        await replica.start()
        started.append(replica)

        old_record = create_node_record(
            identity=publisher.identity,
            addresses=[f"127.0.0.1:{ports[0]}"],
            sequence_number=5,
            ttl_seconds=180,
        )

        new_record = create_node_record(
            identity=publisher.identity,
            addresses=[f"127.0.0.1:{ports[0]}"],
            sequence_number=6,
            ttl_seconds=3,
        )

        key = node_record_key(old_record.node_id)

        # Сохраняем две версии последовательно.
        assert await replica.dht_storage.put(old_record) == key
        assert await replica.dht_storage.put(new_record) == key

        assert await replica.dht_storage.get(key) == new_record

        # Ждём истечения TTL версии 6.
        delay = max(0.0, new_record.expires_at - time.time() + 1.1)
        await __import__("asyncio").sleep(delay)

        await replica.dht_storage.cleanup()

        assert await replica.dht_storage.get(key) is None

        # Перезапускаем реплику.
        await replica.stop()
        started.remove(replica)

        restarted = Node(replica_config)
        await restarted.start()
        started.append(restarted)

        # Старая версия 5 ещё действительна,
        # но принимать её уже нельзя.
        with pytest.raises(RecordRollbackError):
            await restarted.dht_storage.put(old_record)

        assert await restarted.dht_storage.get(key) is None

        # Проверяем сохранность журнала версий.
        assert restarted.dht_storage._versions[key][0] == 6

    finally:
        for node in reversed(started):
            await node.stop()
