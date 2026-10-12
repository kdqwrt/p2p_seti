from p2p.protocol.codec import decode_header
from p2p.protocol.constants import (
    HEADER_SIZE,
    MAX_FRAME_PAYLOAD,
    PROTOCOL_VERSION,
)
from p2p.protocol.frame import Frame


class StreamDecoder:
    def __init__(
        self,
        expected_version: int = PROTOCOL_VERSION,
        max_payload: int = MAX_FRAME_PAYLOAD,
    ) -> None:
        self._buffer = bytearray()

        self._expected_version = expected_version
        self._max_payload = max_payload

    def feed_data(
        self,
        data: bytes,
    ) -> list[Frame]:

        self._buffer.extend(data)

        frames: list[Frame] = []

        while True:
            if len(self._buffer) < HEADER_SIZE:
                break

            header = bytes(
                self._buffer[:HEADER_SIZE]
            )

            (
                version,
                message_type,
                flags,
                request_id,
                payload_length,
            ) = decode_header(
                header,
                expected_version=self._expected_version,
                max_payload=self._max_payload,
            )

            frame_size = (
                HEADER_SIZE + payload_length
            )

            if len(self._buffer) < frame_size:
                break

            payload = bytes(
                self._buffer[
                    HEADER_SIZE:frame_size
                ]
            )

            frame = Frame(
                version=version,
                message_type=message_type,
                flags=flags,
                request_id=request_id,
                payload=payload,
            )

            frames.append(frame)

            del self._buffer[:frame_size]

        return frames

    @property
    def buffered_bytes(self) -> int:
        return len(self._buffer)

    def reset(self) -> None:
        self._buffer.clear()