from __future__ import annotations

import asyncio
import json
from typing import Any

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, Message

from common.config import MAX_RETRIES, RABBITMQ_URL, RETRY_BASE_SECONDS

MAIN_EXCHANGE = "pipeline"
DLX_EXCHANGE = "pipeline.dlx"

QUEUE_BINDINGS: dict[str, list[str]] = {
    "audio.jobs": ["job.uploaded"],
    "transcription.segments": ["segment.ready"],
    "translation.jobs": ["transcript.completed"],
    "job.events": [
        "segmentation.completed",
        "segment.transcribed",
        "segment.failed",
        "translation.completed",
        "translation.failed",
        "pipeline.failed",
    ],
}


async def connect_with_retry(attempts: int = 30, delay: float = 2.0):
    last_error: Exception | None = None
    for _ in range(attempts):
        try:
            return await aio_pika.connect_robust(RABBITMQ_URL)
        except Exception as exc:  # pragma: no cover - startup path
            last_error = exc
            await asyncio.sleep(delay)
    raise RuntimeError("RabbitMQ did not become ready") from last_error


async def declare_topology(channel: aio_pika.abc.AbstractChannel) -> None:
    main = await channel.declare_exchange(MAIN_EXCHANGE, ExchangeType.DIRECT, durable=True)
    dlx = await channel.declare_exchange(DLX_EXCHANGE, ExchangeType.DIRECT, durable=True)
    for queue_name, routing_keys in QUEUE_BINDINGS.items():
        queue = await channel.declare_queue(queue_name, durable=True)
        for routing_key in routing_keys:
            await queue.bind(main, routing_key)
        dlq = await channel.declare_queue(f"{queue_name}.dlq", durable=True)
        await dlq.bind(dlx, queue_name)


async def publish(
    channel: aio_pika.abc.AbstractChannel,
    routing_key: str,
    payload: dict[str, Any],
    *,
    headers: dict[str, Any] | None = None,
    exchange_name: str = MAIN_EXCHANGE,
) -> None:
    exchange = await channel.get_exchange(exchange_name)
    body = json.dumps(payload, separators=(",", ":")).encode()
    message = Message(
        body,
        delivery_mode=DeliveryMode.PERSISTENT,
        content_type="application/json",
        message_id=payload.get("event_id"),
        correlation_id=payload.get("job_id"),
        headers=headers or {},
    )
    await exchange.publish(message, routing_key=routing_key)


def decode(message: aio_pika.IncomingMessage) -> dict[str, Any]:
    return json.loads(message.body.decode())


async def retry_or_dead_letter(
    channel: aio_pika.abc.AbstractChannel,
    message: aio_pika.IncomingMessage,
    routing_key: str,
    payload: dict[str, Any],
) -> bool:
    """Return True when republished for retry, False when sent to the DLQ."""
    attempt = int((message.headers or {}).get("retry_count", 0))
    if attempt < MAX_RETRIES:
        await asyncio.sleep(RETRY_BASE_SECONDS * (2**attempt))
        await publish(
            channel,
            routing_key,
            payload,
            headers={"retry_count": attempt + 1},
        )
        return True
    queue_name = next(
        name for name, keys in QUEUE_BINDINGS.items() if routing_key in keys
    )
    await publish(
        channel,
        queue_name,
        payload,
        headers={"retry_count": attempt, "failed_routing_key": routing_key},
        exchange_name=DLX_EXCHANGE,
    )
    return False

