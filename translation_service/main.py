from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from typing import Any

import aio_pika
from common.database import connection, wait_for_postgres
from common.events import event
from common.messaging import connect_with_retry, decode, declare_topology, publish
from translation_service.translator import Translation, Translator


TIMEOUT_SECONDS = float(os.getenv("TRANSLATION_TIMEOUT_SECONDS", "180"))


def initialise_database() -> None:
    wait_for_postgres()
    schema = Path(__file__).with_name("schema.sql").read_text()
    with connection() as conn:
        conn.execute(schema)
        conn.commit()


async def main() -> None:
    await asyncio.to_thread(initialise_database)
    translator = Translator()
    rabbit = await connect_with_retry()
    channel = await rabbit.channel()
    await channel.set_qos(prefetch_count=1)
    await declare_topology(channel)
    queue = await channel.get_queue("translation.jobs")

    async def callback(message: aio_pika.IncomingMessage) -> None:
        payload = decode(message)


        started = time.perf_counter()
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(translator.translate, payload["transcript"]),
                timeout=TIMEOUT_SECONDS,
            )
            await publish(channel, "translation.completed", event(
                "translation.completed",
                job_id=payload["job_id"],
                translation=result.text
            ))
            await message.ack()
        except Exception as exc:
            await publish(
                channel,
                "translation.failed",
                event(
                    "translation.failed",
                    job_id=payload["job_id"],
                    error=str(exc)
                )
            )
            await message.ack()

    await queue.consume(callback)
    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
