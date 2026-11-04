from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from typing import Any

import aio_pika
from common.database import connection, wait_for_postgres
from common.events import event
from common.messaging import connect_with_retry, decode, declare_topology, publish, retry_or_dead_letter
from translation_service.translator import Translation, Translator


TIMEOUT_SECONDS = float(os.getenv("TRANSLATION_TIMEOUT_SECONDS", "180"))


def initialise_database() -> None:
    wait_for_postgres()
    schema = Path(__file__).with_name("schema.sql").read_text()
    with connection() as conn:
        conn.execute(schema)
        conn.commit()


def cached_result(job_id: str, model: str, prompt: str) -> dict[str, Any] | None:
    with connection() as conn:
        row = conn.execute(
            """SELECT * FROM translation_results
               WHERE job_id=%s AND model_version=%s AND prompt_version=%s""",
            (job_id, model, prompt),
        ).fetchone()
        return dict(row) if row else None


def save_result(job_id: str, translator: Translator, result: Translation, elapsed: float) -> None:
    with connection() as conn:
        conn.execute(
            """INSERT INTO translation_results
               (job_id, model_version, prompt_version, translation, processing_seconds,
                input_tokens, output_tokens)
               VALUES (%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (job_id, model_version, prompt_version) DO NOTHING""",
            (
                job_id, translator.model_version, translator.prompt_version, result.text,
                elapsed, result.input_tokens, result.output_tokens,
            ),
        )
        conn.commit()

def completed_event(row: dict[str, Any]) -> dict[str, Any]:
    return event(
        "translation.completed",
        job_id=str(row["job_id"]),
        translation=row["translation"],
        model_version=row["model_version"],
        prompt_version=row["prompt_version"],
        processing_seconds=row["processing_seconds"],
        input_tokens=row["input_tokens"],
        output_tokens=row["output_tokens"],
    )


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
        cached = await asyncio.to_thread(
            cached_result, payload["job_id"], translator.model_version, translator.prompt_version
        )
        if cached:
            await publish(channel, "translation.completed", completed_event(cached))
            await message.ack()
            return

        started = time.perf_counter()
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(translator.translate, payload["transcript"]),
                timeout=TIMEOUT_SECONDS,
            )
            elapsed = time.perf_counter() - started
            await asyncio.to_thread(save_result, payload["job_id"], translator, result, elapsed)
            row = {
                "job_id": payload["job_id"], "translation": result.text,
                "model_version": translator.model_version, "prompt_version": translator.prompt_version,
                "processing_seconds": elapsed, "input_tokens": result.input_tokens,
                "output_tokens": result.output_tokens,
            }

            await publish(channel, "translation.completed", completed_event(row))
            await message.ack()
        except Exception as exc:
            retried = await retry_or_dead_letter(channel, message, "transcript.completed", payload)
            if not retried:
                await publish(
                    channel,
                    "translation.failed",
                    event(
                        "translation.failed", job_id=payload["job_id"],
                        model_version=translator.model_version,
                        prompt_version=translator.prompt_version, error=str(exc),
                    ),
                )
            await message.ack()

    await queue.consume(callback)
    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
