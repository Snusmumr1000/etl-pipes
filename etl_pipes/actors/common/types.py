from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, NewType, Protocol


@dataclass(frozen=True)
class Message:
    data: Any
    trace_id: MessageTraceId = field(
        default_factory=lambda: MessageTraceId(str(uuid.uuid4()))
    )
    id: MessageId = field(default_factory=lambda: MessageId(str(uuid.uuid4())))
    sender_id: ActorId | None = None
    receiver_id: ActorId | None = None
    sender_name: str | None = None
    receiver_name: str | None = None

    def copy_with_trace_and_data(self, trace_id: MessageTraceId) -> Message:
        return Message(
            data=self.data,
            trace_id=trace_id,
            id=self.id,
            sender_id=self.sender_id,
            receiver_id=self.receiver_id,
        )


MessageTraceId = NewType("MessageTraceId", str)
MessageId = NewType("MessageId", str)
ActorId = NewType("ActorId", str)


class OutputType(Enum):
    RESULT = "result"
    EXCEPTION = "exception"


@dataclass
class Output:
    results: list[Any] = field(default_factory=list)
    exceptions: list[Exception] = field(default_factory=list)

    def save_result(self, result: Any) -> Output:
        self.results.append(result)
        return self

    def save_exception(self, exception: Exception) -> Output:
        self.exceptions.append(exception)
        return self


class IActorSystem(Protocol):
    pass
