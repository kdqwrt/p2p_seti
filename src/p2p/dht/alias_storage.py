
import asyncio
import hashlib
import json
import os
import time
from pathlib import Path

from p2p.dht.alias_records import (
    AliasRecord,
    alias_record_key,
    validate_alias_record,
)


class AliasStorageError(Exception):
    """Ошибка постоянного хранилища псевдонимов."""


class AliasRollbackError(AliasStorageError):
    """Попытка отката версии псевдонима."""


class AliasConflictError(AliasStorageError):
    """Конфликт записей одного владельца и версии."""


class AliasStorage:
    """Постоянное хранилище подписанных псевдонимов."""

    def __init__(self, state_dir: str | Path):
        self.directory = Path(state_dir) / "aliases"
        self.directory.mkdir(parents=True, exist_ok=True)

        self.versions_directory = self.directory / "versions"
        self.versions_directory.mkdir(
            parents=True, exist_ok=True
        )

        # Ключ: (DHT-ключ имени, NodeID владельца).
        self._records: dict[
            tuple[bytes, bytes], AliasRecord
        ] = {}

        # Максимальная принятая версия сохраняется
        # даже после истечения TTL записи.
        self._versions: dict[
            tuple[bytes, bytes], tuple[int, str]
        ] = {}

        self._lock = asyncio.Lock()
        self._load_versions()

    @staticmethod
    def _digest(record: AliasRecord) -> str:
        return hashlib.sha256(record.to_bytes()).hexdigest()

    @staticmethod
    def _filename(key: bytes, owner: bytes) -> str:
        return f"{key.hex()}_{owner.hex()}.json"

    @staticmethod
    def _parse_filename(filename: str) -> tuple[bytes, bytes]:
        parts = filename.removesuffix(".json").split("_")
        if len(parts) != 2:
            raise ValueError("Invalid filename")

        key, owner = (bytes.fromhex(part) for part in parts)
        if len(key) != 32 or len(owner) != 32:
            raise ValueError("Invalid key or owner")
        return key, owner

    def _record_path(self, key: bytes, owner: bytes) -> Path:
        return self.directory / self._filename(key, owner)

    def _version_path(self, key: bytes, owner: bytes) -> Path:
        return self.versions_directory / self._filename(
            key, owner
        )

    @staticmethod
    def _atomic_write(path: Path, data: dict) -> None:
        temporary = path.with_suffix(".tmp")
        try:
            with temporary.open("w", encoding="utf-8") as file:
                json.dump(data, file, indent=2)
                file.flush()
                os.fsync(file.fileno())

            os.replace(temporary, path)
        except OSError as exc:
            raise AliasStorageError(
                f"Cannot persist alias data: {path}"
            ) from exc

    def _load_versions(self) -> None:
        self._versions.clear()

        for path in self.versions_directory.glob("*.json"):
            try:
                key, owner = self._parse_filename(path.name)
                data = json.loads(path.read_text(encoding="utf-8"))

                sequence = data["sequence_number"]
                digest = data["digest"]

                if (
                    type(sequence) is not int
                    or not 0 <= sequence < 2**64
                    or type(digest) is not str
                    or len(digest) != 64
                    or any(c not in "0123456789abcdef" for c in digest)
                ):
                    raise ValueError("Invalid version metadata")

                self._versions[(key, owner)] = (
                    sequence, digest
                )
            except (OSError, ValueError, KeyError, TypeError) as exc:
                raise AliasStorageError(
                    f"Cannot load alias version metadata: {path}"
                ) from exc

    def _write_version(
        self,
        key: bytes,
        owner: bytes,
        sequence: int,
        digest: str,
    ) -> None:
        self._atomic_write(
            self._version_path(key, owner),
            {"sequence_number": sequence, "digest": digest},
        )

    def _write_record(
        self,
        key: bytes,
        record: AliasRecord,
    ) -> None:
        self._atomic_write(
            self._record_path(key, record.node_id),
            {"record": record.to_bytes().hex()},
        )

    def _delete_record(self, key: bytes, owner: bytes) -> None:
        try:
            self._record_path(key, owner).unlink(missing_ok=True)
        except OSError as exc:
            raise AliasStorageError(
                "Cannot delete alias record"
            ) from exc

    async def load(self) -> int:
        """Восстанавливает действующие записи с диска."""
        async with self._lock:
            self._load_versions()
            self._records.clear()
            loaded = 0

            for path in self.directory.glob("*.json"):
                try:
                    key, owner = self._parse_filename(path.name)
                    data = json.loads(
                        path.read_text(encoding="utf-8")
                    )

                    record = AliasRecord.from_bytes(
                        bytes.fromhex(data["record"])
                    )
                    validate_alias_record(record)

                    expected_key = alias_record_key(record.alias)
                    if key != expected_key or owner != record.node_id:
                        continue

                    identifier = (key, owner)
                    digest = self._digest(record)
                    previous = self._versions.get(identifier)

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
                        self._write_version(
                            key, owner,
                            record.sequence_number, digest
                        )
                        self._versions[identifier] = (
                            record.sequence_number, digest
                        )

                    self._records[identifier] = record
                    loaded += 1

                except (
                    OSError, ValueError, KeyError,
                    TypeError, UnicodeError
                ):
                    # Некорректная или просроченная запись
                    # не восстанавливается.
                    continue

            return loaded

    async def put(
        self,
        record: AliasRecord,
        now: int | None = None,
    ) -> bytes:
        """Проверяет и сохраняет подписанный псевдоним."""
        validate_alias_record(record, now=now)

        key = alias_record_key(record.alias)
        owner = record.node_id
        identifier = (key, owner)
        digest = self._digest(record)

        async with self._lock:
            previous = self._versions.get(identifier)

            if previous is not None:
                old_sequence, old_digest = previous

                if record.sequence_number < old_sequence:
                    raise AliasRollbackError(
                        "Alias sequence number rollback"
                    )

                if (
                    record.sequence_number == old_sequence
                    and digest != old_digest
                ):
                    raise AliasConflictError(
                        "Conflicting alias record at same sequence"
                    )

            # Журнал обновляется раньше записи:
            # после сбоя старая версия не сможет
            # перезаписать уже принятую новую.
            if (
                previous is None
                or record.sequence_number > previous[0]
            ):
                self._write_version(
                    key, owner,
                    record.sequence_number, digest
                )
                self._versions[identifier] = (
                    record.sequence_number, digest
                )

            self._write_record(key, record)
            self._records[identifier] = record

        return key

    async def get_all(
        self,
        alias: str,
        now: int | None = None,
    ) -> list[AliasRecord]:
        """Возвращает действующие заявки всех владельцев."""
        key = alias_record_key(alias)
        current = int(time.time()) if now is None else now

        async with self._lock:
            expired = [
                identifier
                for identifier, record in self._records.items()
                if identifier[0] == key
                and record.expires_at <= current
            ]

            for identifier in expired:
                self._delete_record(*identifier)
                self._records.pop(identifier, None)

            records = [
                record
                for identifier, record in self._records.items()
                if identifier[0] == key
            ]

            # Детерминированное правило выбора.
            records.sort(
                key=lambda record: hashlib.sha256(
                    b"alias-owner:"
                    + key
                    + record.node_id
                ).digest()
            )
            return records

    async def get(
        self,
        alias: str,
        now: int | None = None,
    ) -> AliasRecord | None:
        """Возвращает предпочтительную действующую запись."""
        records = await self.get_all(alias, now=now)
        return records[0] if records else None

    async def cleanup(self, now: int | None = None) -> int:
        """Удаляет просроченные записи, сохраняя версии."""
        current = int(time.time()) if now is None else now

        async with self._lock:
            expired = [
                identifier
                for identifier, record in self._records.items()
                if record.expires_at <= current
            ]

            for identifier in expired:
                self._delete_record(*identifier)
                self._records.pop(identifier, None)

            return len(expired)

    async def count(self) -> int:
        async with self._lock:
            return len(self._records)
