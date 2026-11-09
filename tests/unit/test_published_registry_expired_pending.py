
import time

import pytest

from p2p.dht.published_registry import PublishedRecordRegistry
from p2p.dht.records import (
    node_record_key,
)


@pytest.mark.asyncio
async def test_expired_record_with_pending_survives_cleanup(
    tmp_path,
):

    registry = PublishedRecordRegistry(str(tmp_path))

    assert await registry.cleanup() == 0
    assert await registry.list_active() == []
