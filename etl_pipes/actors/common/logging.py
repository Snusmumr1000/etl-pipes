from __future__ import annotations

import logging
from datetime import datetime

from etl_pipes.actors.common.types import Message

actor_logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)


def log_message(
    level: int,
    message: Message,
    text: str,
    log_data: bool,
) -> None:
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    actor_logger.log(
        level,
        f"{current_time} [mid:%s][tid:%s][f:%s][t:%s][d:%s] %s",
        message.trace_id,
        message.id,
        message.sender_name or message.sender_id,
        message.receiver_name or message.receiver_id,
        log_data and message.data or "",
        text,
    )


def log_message_info(message: Message, text: str, log_data: bool = False) -> None:
    log_message(logging.INFO, message, text, log_data)
