
import asyncio
import hashlib
import json
import os
import time
from pathlib import Path

from p2p.dht.records import (
    NodeRecord,
    RecordError,
    node_record_key,
    validate_node_record,
)


class StorageError(Exception):
    """Ошибка локального DHT-хранилища."""


class RecordRollbackError(StorageError):
    """Попытка сохранить устаревшую версию записи."""


class RecordConflictError(StorageError):
    """Конфликт владельцев или содержимого записи."""


class DHTStorage:
    """Постоянное хранилище подписанных NodeRecord."""

    def __init__(self, state_dir: str | Path):
        self.directory = Path(state_dir) / "dht"
        self.directory.mkdir(parents=True, exist_ok=True)

        # Журнал версий не очищается вместе с записями.
        self.versions_directory = self.directory / "versions"
        self.versions_directory.mkdir(
            parents=True, exist_ok=True
        )

        self._records: dict[bytes, NodeRecord] = {}
        self._versions: dict[bytes, tuple[int, str]] = {}
        self._lock = asyncio.Lock()

        # Восстанавливаем журнал уже при создании объекта.
        self._load_versions()

    @staticmethod
    def _digest(record: NodeRecord) -> str:
        return hashlib.sha256(record.to_bytes()).hexdigest()

    def _version_path(self, key: bytes) -> Path:
        return self.versions_directory / f"{key.hex()}.json"

    def _load_versions(self) -> None:
        self._versions.clear()

        for path in self.versions_directory.glob("*.json"):
            try:
                key = bytes.fromhex(path.stem)

                if len(key) != 32:
                    raise ValueError("Invalid version key")

                data = json.loads(
                    path.read_text(encoding="utf-8")
                )

                sequence = data["sequence_number"]
                digest = data["digest"]

                if (
                    type(sequence) is not int
                    or not 0 <= sequence < 2**64
                    or not isinstance(digest, str)
                    or len(digest) != 64
                    or any(c not in "0123456789abcdef"
                           for c in digest)
                ):
                    raise ValueError("Invalid version metadata")

                self._versions[key] = (sequence, digest)

            except (OSError, ValueError, KeyError, TypeError) as exc:
                # Не игнорируем повреждение журнала безопасности.
                raise StorageError(
                    f"Cannot load DHT version metadata: {path}"
                ) from exc

    def _write_version(
            self,
            key: bytes,
            sequence: int,
            digest: str,
    ) -> None:
        path = self._version_path(key)
        temporary = path.with_suffix(".tmp")

        data = {
            "sequence_number": sequence,
            "digest": digest,
        }

        try:
            with temporary.open("w", encoding="utf-8") as file:
                json.dump(data, file, indent=2)
                file.flush()
                os.fsync(file.fileno())

            os.replace(temporary, path)

        except OSError as exc:
            raise StorageError(
                "Cannot persist DHT version metadata"
            ) from exc

    async def load(self) -> int:
        """Загружает действующие записи с диска."""

        async with self._lock:
            self._load_versions()
            self._records.clear()
            loaded = 0

            for path in self.directory.glob("*.json"):
                try:
                    data = json.loads(
                        path.read_text(encoding="utf-8")
                    )

                    record = NodeRecord.from_bytes(
                        bytes.fromhex(data["record"])
                    )

                    validate_node_record(record)

                    key = node_record_key(record.node_id)

                    if path.stem != key.hex():
                        raise StorageError(
                            "Incorrect record filename"
                        )

                    digest = self._digest(record)
                    previous = self._versions.get(key)

                    if previous is not None:
                        old_sequence, old_digest = previous

                        if record.sequence_number < old_sequence:
                            continue

                        if (
                            record.sequence_number == old_sequence
                            and digest != old_digest
                        ):
                            continue

                    if (
                        previous is None
                        or record.sequence_number > previous[0]
                    ):
                        # Поддержка записей, созданных до
                        # появления постоянного журнала.
                        self._write_version(
                            key,
                            record.sequence_number,
                            digest,
                        )
                        self._versions[key] = (
                            record.sequence_number,
                            digest,
                        )

                    self._records[key] = record
                    loaded += 1

                except (
                    OSError,
                    ValueError,
                    KeyError,
                    TypeError,
                    RecordError,
                ):
                    # Повреждённая или просроченная запись
                    # не попадает в активное хранилище.
                    continue

            return loaded

    async def put(
        self,
        record: NodeRecord,
        now: int | None = None,
    ) -> bytes:
        """Проверяет и сохраняет подписанную запись."""

        validate_node_record(record, now=now)

        key = node_record_key(record.node_id)
        digest = self._digest(record)

        async with self._lock:
            previous = self._versions.get(key)

            if previous is not None:
                old_sequence, old_digest = previous

                if record.sequence_number < old_sequence:
                    raise RecordRollbackError(
                        "Record sequence number rollback"
                    )

                if (
                    record.sequence_number == old_sequence
                    and digest != old_digest
                ):
                    raise RecordConflictError(
                        "Conflicting record with same sequence number"
                    )

            existing = self._records.get(key)

            if (
                existing is not None
                and existing.identity_public_key
                != record.identity_public_key
            ):
                raise RecordConflictError(
                    "Different owner for the same DHT key"
                )

            # Сначала фиксируем максимальную версию.
            # Это исключает потерю защиты при сбое
            # между записью метаданных и самой записи.
            if (
                previous is None
                or record.sequence_number > previous[0]
            ):
                self._write_version(
                    key,
                    record.sequence_number,
                    digest,
                )
                self._versions[key] = (
                    record.sequence_number,
                    digest,
                )

            self._write_record(key, record)
            self._records[key] = record

        return key

    async def get(
        self,
        key: bytes,
        now: int | None = None,
    ) -> NodeRecord | None:
        """Возвращает действующую запись."""

        current = int(time.time()) if now is None else now

        async with self._lock:
            record = self._records.get(key)

            if record is None:
                return None

            if record.expires_at <= current:
                self._delete_record(key)
                self._records.pop(key, None)
                return None

            return record

    async def cleanup(
        self,
        now: int | None = None,
    ) -> int:
        """Удаляет просроченные записи, сохраняя журнал."""

        current = int(time.time()) if now is None else now

        async with self._lock:
            expired = [
                key
                for key, record in self._records.items()
                if record.expires_at <= current
            ]

            for key in expired:
                self._delete_record(key)
                self._records.pop(key, None)

            return len(expired)

    async def count(self) -> int:
        async with self._lock:
            return len(self._records)

    def _write_record(
        self,
        key: bytes,
        record: NodeRecord,
    ) -> None:
        path = self.directory / f"{key.hex()}.json"
        temporary = path.with_suffix(".tmp")

        data = {
            "key": key.hex(),
            "record": record.to_bytes().hex(),
        }

        try:
            temporary.write_text(
                json.dumps(data, indent=2),
                encoding="utf-8",
            )
            os.replace(temporary, path)

        except OSError as exc:
            raise StorageError(
                "Cannot persist DHT record"
            ) from exc

    def _delete_record(self, key: bytes) -> None:
        path = self.directory / f"{key.hex()}.json"

        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            raise StorageError(
                "Cannot delete DHT record"
            ) from exc
