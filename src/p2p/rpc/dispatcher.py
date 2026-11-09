
import time
from collections.abc import Callable

from p2p.dht.alias_records import AliasRecord, AliasRecordError, alias_record_key
from p2p.dht.alias_storage import (
    AliasStorageError,
    AliasRollbackError,
    AliasConflictError,
)
from p2p.dht.alias_messages import (
    decode_store_alias_request,
    encode_store_alias_response,
    decode_find_alias_request,
    encode_find_alias_response,
)

from p2p.protocol.constants import MessageType
from p2p.protocol.contact import Contact
from p2p.protocol.frame import Frame
from p2p.protocol.messages import (
    decode_ping,
    encode_error,
    encode_pong,
    decode_find_node_request,
    encode_find_node_response,
)
from p2p.protocol.payload_codec import PayloadError

from collections.abc import Awaitable

from p2p.dht.records import NodeRecord, RecordError, node_record_key
from p2p.dht.storage import (
    RecordRollbackError,
    RecordConflictError,
    StorageError,
)
from p2p.dht.messages import (
    decode_store_request,
    encode_store_response,
    decode_find_value_request,
    encode_find_value_response,
)

class Dispatcher:


    def __init__(
        self,
        local_contact: Contact,
        on_ping: Callable[[Contact], None] | None = None,
        on_find_node: Callable[[bytes], list[Contact]] | None = None,
        on_store: Callable[[NodeRecord], Awaitable[bytes]] | None = None,
        on_find_value: Callable[
            [bytes], Awaitable[NodeRecord | None]
        ] | None = None,
        on_store_alias: Callable[
            [AliasRecord], Awaitable[bytes]
         ] | None = None,
        on_find_alias: Callable[
            [str], Awaitable[list[AliasRecord]]
        ] | None = None,
    ) -> None:
        self.local_contact = local_contact
        self.on_ping = on_ping
        self.on_find_node = on_find_node
        self.on_store = on_store
        self.on_find_value = on_find_value
        self.on_store_alias = on_store_alias
        self.on_find_alias = on_find_alias



    async def dispatch(self, frame: Frame) -> Frame:
        if frame.message_type == MessageType.PING:
            return self._handle_ping(frame)

        if frame.message_type in {
            MessageType.PONG,
            MessageType.FIND_NODE_RESPONSE,
            MessageType.STORE_RESPONSE,
            MessageType.FIND_VALUE_RESPONSE,
            MessageType.ERROR,
            MessageType.STORE_ALIAS_RESPONSE,
            MessageType.FIND_ALIAS_RESPONSE,
        }:
            return self._make_error(
                frame,
                code="INVALID_DIRECTION",
                message="Response message is not valid as a request",
            )

        if frame.message_type == MessageType.FIND_NODE_REQUEST:
            if self.on_find_node is None:
                return self._make_error(
                    frame,
                    code="NOT_IMPLEMENTED",
                    message="FIND_NODE is not implemented",
                )

            return self._handle_find_node(frame)

        if frame.message_type == MessageType.STORE_REQUEST:
            return await self._handle_store(frame)

        if frame.message_type == MessageType.FIND_VALUE_REQUEST:
            return await self._handle_find_value(frame)


        if frame.message_type == MessageType.STORE_ALIAS_REQUEST:
            return await self._handle_store_alias(frame)

        if frame.message_type == MessageType.FIND_ALIAS_REQUEST:
            return await self._handle_find_alias(frame)

        return self._make_error(
            frame,
            code="UNSUPPORTED_MESSAGE",
            message="Unsupported message type",
        )


    def _handle_ping(self, frame: Frame) -> Frame:
        ping = decode_ping(frame.payload)

        if self.on_ping is not None:
            self.on_ping(ping["sender"])

        payload = encode_pong(
            responder=self.local_contact,
            ping_timestamp_ms=ping["timestamp_ms"],
            responder_timestamp_ms=int(time.time() * 1000),
        )

        return Frame(
            version=frame.version,
            message_type=MessageType.PONG,
            flags=0,
            request_id=frame.request_id,
            payload=payload,
        )




    def _handle_find_node(self, frame: Frame) -> Frame:
        request = decode_find_node_request(frame.payload)

        sender = request["sender"]
        target_node_id = request["target_node_id"]

        if sender.node_id == self.local_contact.node_id:
            raise PayloadError("Sender cannot be local node")

        contacts = self.on_find_node(target_node_id)

        payload = encode_find_node_response(
            responder=self.local_contact,
            target_node_id=target_node_id,
            contacts=contacts,
        )

        return Frame(
            version=frame.version,
            message_type=MessageType.FIND_NODE_RESPONSE,
            flags=0,
            request_id=frame.request_id,
            payload=payload,
        )


    async def _handle_store(self, frame: Frame) -> Frame:
        if self.on_store is None:
            return self._make_error(
                frame, "NOT_IMPLEMENTED", "STORE is not implemented"
            )

        request = decode_store_request(frame.payload)
        key = request["key"]
        record = request["record"]

        if key != node_record_key(record.node_id):
            raise PayloadError("DHT key mismatch")

        try:
            stored_key = await self.on_store(record)
        except RecordRollbackError:
            return self._make_error(
                frame, "ROLLBACK", "Older record version"
            )
        except RecordConflictError:
            return self._make_error(
                frame, "CONFLICT", "Conflicting record"
            )
        except RecordError:
            return self._make_error(
                frame, "INVALID_RECORD", "Invalid signed record"
            )
        except StorageError:
            return self._make_error(
                frame, "STORAGE_ERROR", "Cannot persist record"
            )

        if stored_key != key:
            return self._make_error(
                frame, "STORAGE_ERROR", "Stored key mismatch"
            )

        return Frame(
            version=frame.version,
            message_type=MessageType.STORE_RESPONSE,
            flags=0,
            request_id=frame.request_id,
            payload=encode_store_response(key, True),
        )

    async def _handle_find_value(self, frame: Frame) -> Frame:
        if self.on_find_value is None:
            return self._make_error(
                frame, "NOT_IMPLEMENTED", "FIND_VALUE is not implemented"
            )

        request = decode_find_value_request(frame.payload)
        key = request["key"]

        try:
            record = await self.on_find_value(key)
        except StorageError:
            return self._make_error(
                frame, "STORAGE_ERROR", "Cannot read record"
            )

        return Frame(
            version=frame.version,
            message_type=MessageType.FIND_VALUE_RESPONSE,
            flags=0,
            request_id=frame.request_id,
            payload=encode_find_value_response(key, record),
        )


    async def _handle_store_alias(self, frame: Frame) -> Frame:
        if self.on_store_alias is None:
            return self._make_error(
                frame, "NOT_IMPLEMENTED", "STORE_ALIAS is not implemented"
            )

        request = decode_store_alias_request(frame.payload)
        key = request["key"]
        record = request["record"]

        try:
            stored_key = await self.on_store_alias(record)
        except AliasRollbackError:
            return self._make_error(
                frame, "ROLLBACK", "Older alias version"
            )
        except AliasConflictError:
            return self._make_error(
                frame, "CONFLICT", "Conflicting alias record"
            )
        except AliasRecordError:
            return self._make_error(
                frame, "INVALID_RECORD", "Invalid signed alias"
            )
        except AliasStorageError:
            return self._make_error(
                frame, "STORAGE_ERROR", "Cannot persist alias"
            )

        if stored_key != key:
            return self._make_error(
                frame, "STORAGE_ERROR", "Stored alias key mismatch"
            )

        return Frame(
            version=frame.version,
            message_type=MessageType.STORE_ALIAS_RESPONSE,
            flags=0,
            request_id=frame.request_id,
            payload=encode_store_alias_response(key, True),
        )

    async def _handle_find_alias(self, frame: Frame) -> Frame:
        if self.on_find_alias is None:
            return self._make_error(
                frame, "NOT_IMPLEMENTED", "FIND_ALIAS is not implemented"
            )

        request = decode_find_alias_request(frame.payload)
        alias = request["alias"]

        try:
            records = await self.on_find_alias(alias)
        except AliasStorageError:
            return self._make_error(
                frame, "STORAGE_ERROR", "Cannot read alias"
            )

        return Frame(
            version=frame.version,
            message_type=MessageType.FIND_ALIAS_RESPONSE,
            flags=0,
            request_id=frame.request_id,
            payload=encode_find_alias_response(alias, records),
        )


    @staticmethod
    def _make_error(
        frame: Frame,
        code: str,
        message: str,
    ) -> Frame:
        return Frame(
            version=frame.version,
            message_type=MessageType.ERROR,
            flags=0,
            request_id=frame.request_id,
            payload=encode_error(
                code=code,
                message=message,
            ),
        )
