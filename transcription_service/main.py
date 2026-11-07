from __future__ import annotations

import asyncio
import os
import tempfile
import time
from pathlib import Path
from typing import Any

import aio_pika
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from common.database import connection, wait_for_postgres
from common.events import event
from common.messaging import (
    connect_with_retry,
    decode,
    declare_topology,
    publish,
    retry_or_dead_letter,
)
from common.storage import download_file, wait_for_storage
from transcription_service.transcriber import Transcriber


CONCURRENCY = int(os.getenv("TRANSCRIPTION_CONCURRENCY", "3"))
TIMEOUT_SECONDS = float(os.getenv("TRANSCRIPTION_TIMEOUT_SECONDS", "180"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

RESULTS = Counter("transcription_segments_total", "Transcription segment results", ["result", "model"])
LATENCY = Histogram("transcription_processing_seconds", "Whisper processing time", ["model"])
REALTIME_FACTOR = Histogram(
    "transcription_realtime_factor", "Processing seconds divided by audio seconds", ["model"]
)
IN_PROGRESS = Gauge("transcription_in_progress", "Segments currently being transcribed")


def initialise_database() -> None:
    wait_for_postgres()
    schema = Path(__file__).with_name("schema.sql").read_text()
    with connection() as conn:
        conn.execute(schema)
        conn.commit()


def cached_result(job_id: str, number: int, model_version: str) -> dict[str, Any] | None:
    with connection() as conn:
        row = conn.execute(
            """
            SELECT * FROM transcription_results
            WHERE job_id=%s AND segment_number=%s AND model_version=%s
            """,
            (job_id, number, model_version),
        ).fetchone()
        return dict(row) if row else None


def save_result(payload: dict[str, Any]) -> None:
    with connection() as conn:
        conn.execute(
            """
            INSERT INTO transcription_results
                (job_id, segment_number, model_version, text, start_ms, end_ms, processing_seconds)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (job_id, segment_number, model_version) DO NOTHING
            """,
            (
                payload["job_id"],
                payload["segment_number"],
                payload["model_version"],
                payload["text"],
                payload["start_ms"],
                payload["end_ms"],
                payload["processing_seconds"],
            ),
        )
        conn.commit()


async def main() -> None:
    await asyncio.to_thread(initialise_database)
    storage = await asyncio.to_thread(wait_for_storage)
    transcriber = Transcriber()
    start_http_server(METRICS_PORT)
    rabbit = await connect_with_retry()
    channel = await rabbit.channel()
    await channel.set_qos(prefetch_count=CONCURRENCY)
    await declare_topology(channel)
    queue = await channel.get_queue("transcription.segments")
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async def callback(message: aio_pika.IncomingMessage) -> None:
        payload = decode(message)
        async with semaphore:
            model_version = transcriber.model_version
            cached = await asyncio.to_thread(
                cached_result,
                payload["job_id"],
                payload["segment_number"],
                model_version,
            )
            if cached:
                await publish(
                    channel,
                    "segment.transcribed",
                    event(
                        "segment.transcribed",
                        job_id=cached["job_id"],
                        segment_number=cached["segment_number"],
                        start_ms=cached["start_ms"],
                        end_ms=cached["end_ms"],
                        text=cached["text"],
                        model_version=cached["model_version"],
                        processing_seconds=cached["processing_seconds"],
                    ),
                )
                await message.ack()
                return

            temp_path = None
            start = time.perf_counter()
            IN_PROGRESS.inc()
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as temp:
                    temp_path = Path(temp.name)
                await asyncio.to_thread(download_file, storage, payload["object_name"], temp_path)
                text = await asyncio.wait_for(
                    asyncio.to_thread(
                        transcriber.transcribe,
                        temp_path,
                        payload["segment_number"],
                    ),
                    timeout=TIMEOUT_SECONDS,
                )
                elapsed = time.perf_counter() - start
                result = {
                    "job_id": payload["job_id"],
                    "segment_number": payload["segment_number"],
                    "start_ms": payload["start_ms"],
                    "end_ms": payload["end_ms"],
                    "text": text,
                    "model_version": model_version,
                    "processing_seconds": elapsed,
                }
                await asyncio.to_thread(save_result, result)
                await publish(channel, "segment.transcribed", event("segment.transcribed", **result))
                RESULTS.labels(result="success", model=model_version).inc()
                LATENCY.labels(model=model_version).observe(elapsed)
                REALTIME_FACTOR.labels(model=model_version).observe(
                    elapsed / max(float(payload["audio_seconds"]), 0.001)
                )
                await message.ack()
            except Exception as exc:
                retried = await retry_or_dead_letter(channel, message, "segment.ready", payload)
                if not retried:
                    RESULTS.labels(result="failed", model=model_version).inc()
                    await publish(
                        channel,
                        "segment.failed",
                        event(
                            "segment.failed",
                            job_id=payload["job_id"],
                            segment_number=payload["segment_number"],
                            start_ms=payload["start_ms"],
                            end_ms=payload["end_ms"],
                            model_version=model_version,
                            processing_seconds=time.perf_counter() - start,
                            error=str(exc),
                        ),
                    )
                await message.ack()
            finally:
                IN_PROGRESS.dec()
                if temp_path:
                    temp_path.unlink(missing_ok=True)

    await queue.consume(callback)
    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
