from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from typing import Any

import aio_pika

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from common.database import connection, wait_for_postgres
from common.events import event
from common.messaging import connect_with_retry, decode, declare_topology, publish, retry_or_dead_letter
from translation_service.translator import Translation, Translator


TIMEOUT_SECONDS = float(os.getenv("TRANSLATION_TIMEOUT_SECONDS", "180"))

FAILURE_THRESHOLD = int(os.getenv("CIRCUIT_FAILURE_THRESHOLD", "3"))
RESET_SECONDS = float(os.getenv("CIRCUIT_RESET_SECONDS", "30"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

RESULTS = Counter("translation_jobs_total", "Translation results", ["result", "model"])
LATENCY = Histogram("translation_processing_seconds", "Translation processing time", ["model"])
TOKENS = Counter("translation_tokens_total", "Approximate or model-reported tokens", ["kind", "model"])
CIRCUIT_OPEN = Gauge("translation_circuit_open", "1 while the translation circuit is open")

class CircuitBreaker:
    def __init__(self) -> None:
        self.failures = 0
        self.opened_at: float | None = None

    def allow(self) -> bool:
        if self.opened_at is None:
            return True
        if time.monotonic() - self.opened_at >= RESET_SECONDS:
            self.opened_at = None
            self.failures = 0
            CIRCUIT_OPEN.set(0)
            return True
        return False

    def success(self) -> None:
        self.failures = 0
        self.opened_at = None
        CIRCUIT_OPEN.set(0)

    def failure(self) -> None:
        self.failures += 1
        if self.failures >= FAILURE_THRESHOLD:
            self.opened_at = time.monotonic()
            CIRCUIT_OPEN.set(1)


class CircuitOpenError(RuntimeError):
    pass


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
    breaker = CircuitBreaker()
    start_http_server(METRICS_PORT)

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
            if not breaker.allow():
                raise CircuitOpenError("translation circuit is open")

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

            breaker.success()
            RESULTS.labels(result="success", model=translator.model_version).inc()
            LATENCY.labels(model=translator.model_version).observe(elapsed)
            TOKENS.labels(kind="input", model=translator.model_version).inc(result.input_tokens)
            TOKENS.labels(kind="output", model=translator.model_version).inc(result.output_tokens)

            await message.ack()
        except Exception as exc:
            if not isinstance(exc, CircuitOpenError):
                breaker.failure()

            retried = await retry_or_dead_letter(channel, message, "transcript.completed", payload)
            if not retried:

                RESULTS.labels(result="failed", model=translator.model_version).inc()

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
