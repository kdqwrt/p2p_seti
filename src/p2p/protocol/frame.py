from dataclasses import dataclass

from .constants import MessageType


@dataclass(slots=True)
class Frame:
    version: int
    message_type: MessageType
    flags: int
    request_id: bytes
    payload: bytes

    def __post_init__(self) -> None:
        if not 0 <= self.version <= 255:
            raise ValueError("version must fit into 1 byte")

        if not 0 <= self.flags <= 0xFFFF:
            raise ValueError("flags must fit into 2 bytes")

        if len(self.request_id) != 16:
            raise ValueError("request_id must contain exactly 16 bytes")