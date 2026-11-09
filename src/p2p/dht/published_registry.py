
import asyncio
import os
import time
from pathlib import Path

from p2p.dht.records import (
    NodeRecord,
    node_record_key,
    validate_node_record,
)


class PublishedRecordRegistry:


    def __init__(self, state_dir: str):
        self.directory = (
            Path(state_dir) / "published_records"
        )

        self._records: dict[bytes, NodeRecord] = {}
        self._lock = asyncio.Lock()

    def _record_path(self, key: bytes) -> Path:
        return self.directory / f"{key.hex()}.bin"

    async def add(self, record: NodeRecord) -> bool:


        validate_node_record(record)

        key = node_record_key(record.node_id)

        async with self._lock:
            current = self._records.get(key)

            if current is not None:
                if (
                    record.sequence_number
                    < current.sequence_number
                ):
                    return False

                if (
                    record.sequence_number
                    == current.sequence_number
                    and record != current
                ):
                    return False

            self.directory.mkdir(
                parents=True,
                exist_ok=True,
            )

            path = self._record_path(key)
            temporary_path = path.with_suffix(".tmp")

            try:
                temporary_path.write_bytes(
                    record.to_bytes()
                )

                os.replace(
                    temporary_path,
                    path,
                )

            finally:
                temporary_path.unlink(
                    missing_ok=True
                )

            self._records[key] = record

            return True

    async def get(
        self,
        key: bytes,
    ) -> NodeRecord | None:

        async with self._lock:
            record = self._records.get(key)

            if record is None:
                return None

            if record.expires_at <= int(time.time()):
                return None

            return record

    async def list_active(self) -> list[NodeRecord]:

        async with self._lock:
            now = int(time.time())

            return [
                record
                for record in self._records.values()
                if record.expires_at > now
            ]

    async def cleanup(self) -> int:

        async with self._lock:
            return 0

    async def load(self) -> int:

        async with self._lock:
            self.directory.mkdir(
                parents=True,
                exist_ok=True,
            )

            loaded: dict[bytes, NodeRecord] = {}

            for path in self.directory.glob("*.bin"):
                try:
                    record = NodeRecord.from_bytes(
                        path.read_bytes()
                    )

                    self._validate_saved_record(record)

                    key = node_record_key(
                        record.node_id
                    )


                    if path.name != f"{key.hex()}.bin":
                        continue

                    

                    loaded[key] = record

                except (
                    ValueError,
                    TypeError,
                    KeyError,
                    OSError,
                ):

                    continue

            self._records = loaded

            return len(self._records)



    def _pending_path(self, key: bytes) -> Path:
        return self.directory / f"{key.hex()}.pending"

    async def save_pending(self, record: NodeRecord) -> None:
        validate_node_record(record)
        key = node_record_key(record.node_id)

        async with self._lock:
            self.directory.mkdir(parents=True, exist_ok=True)
            path = self._pending_path(key)
            temporary = path.with_suffix(".pending.tmp")

            try:
                temporary.write_bytes(record.to_bytes())
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)

    async def get_pending(self, key: bytes) -> NodeRecord | None:
        async with self._lock:
            path = self._pending_path(key)

            if not path.exists():
                return None

            try:
                record = NodeRecord.from_bytes(path.read_bytes())
                self._validate_saved_record(record)

                if node_record_key(record.node_id) != key:
                    raise ValueError("Pending record key mismatch")

                return record

            except (ValueError, TypeError, OSError):

                path.unlink(missing_ok=True)
                return None

    async def get_stored(
            self,
            key: bytes,
    ) -> NodeRecord | None:

        async with self._lock:
            return self._records.get(key)

    async def clear_pending(self, key: bytes) -> None:
        async with self._lock:
            self._pending_path(key).unlink(missing_ok=True)

    @staticmethod
    def _validate_saved_record(record: NodeRecord) -> None:
        check_time = min(
            int(time.time()),
            record.expires_at - 1,
        )
        validate_node_record(record, now=check_time)

    async def list_stored(self) -> list[NodeRecord]:
        async with self._lock:
            return list(self._records.values())
