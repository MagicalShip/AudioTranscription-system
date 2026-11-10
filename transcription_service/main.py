from __future__ import annotations

import asyncio
import logging
import math
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
    dead_letter_invalid,
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


def save_result(payload: dict[str, Any]) -> dict[str, Any]:
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
    row = cached_result(payload['job_id'], payload['segment_number'], payload['model_version'])
    if row is None:
        raise RuntimeError('Saved transcription result is unavailable')
    return row


LOGGER = logging.getLogger(__name__)
RESULT_FIELDS = ('job_id', 'segment_number', 'start_ms', 'end_ms', 'text',
                 'model_version', 'processing_seconds')


def validate_segment(payload):
    if not isinstance(payload, dict) or payload.get('event_type') != 'segment.ready':
        raise ValueError('Expected a segment.ready object')
    for key in ('job_id', 'object_name'):
        if not isinstance(payload.get(key), str) or not payload[key].strip():
            raise ValueError(f'{key} must be a nonempty string')
    for key in ('segment_number', 'start_ms', 'end_ms'):
        if type(payload.get(key)) is not int or payload[key] < 0:
            raise ValueError(f'{key} must be a nonnegative integer')
    duration = payload.get('audio_seconds')
    if (type(duration) not in (int, float) or (isinstance(duration, float) and not math.isfinite(duration))
            or duration <= 0 or payload['end_ms'] <= payload['start_ms']):
        raise ValueError('Invalid segment duration')
    return payload


async def run_blocking(function, *args, timeout=None):
    # A Python thread cannot be forcibly stopped. Retain its slot and files until
    # it exits, even after the deadline or cancellation, before allowing a retry.
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.wait_for(asyncio.shield(task), timeout)
    except (TimeoutError, asyncio.CancelledError):
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        if not task.cancelled():
            task.exception()  # Retrieve any background failure before propagating.
        raise


async def handle_message(message, channel, storage, transcriber):
    try:
        try:
            payload = validate_segment(decode(message))
        except (ValueError, TypeError, UnicodeError) as exc:
            await dead_letter_invalid(channel, message, 'transcription.segments', str(exc))
            await message.ack()
            return
        model = transcriber.model_version
        row = await run_blocking(cached_result, payload['job_id'], payload['segment_number'], model)
        if row is None:
            started = time.perf_counter()
            try:
                IN_PROGRESS.inc()
            except Exception:
                LOGGER.exception('Unable to record active work')
            try:
                with tempfile.TemporaryDirectory() as directory:
                    path = Path(directory) / 'segment.wav'
                    try:
                        await run_blocking(download_file, storage, payload['object_name'], path)
                        text = await run_blocking(transcriber.transcribe, path,
                                                  payload['segment_number'], timeout=TIMEOUT_SECONDS)
                    except Exception as exc:
                        retried = await retry_or_dead_letter(channel, message, 'segment.ready', payload)
                        if not retried:
                            await publish(channel, 'segment.failed', event(
                                'segment.failed', job_id=payload['job_id'],
                                segment_number=payload['segment_number'], start_ms=payload['start_ms'],
                                end_ms=payload['end_ms'], model_version=model,
                                processing_seconds=time.perf_counter() - started,
                                error=str(exc) or type(exc).__name__))
                            try:
                                RESULTS.labels(result='failed', model=model).inc()
                            except Exception:
                                LOGGER.exception('Unable to record failure metrics')
                        await message.ack()
                        return
                    elapsed = time.perf_counter() - started
                    result = {key: payload[key] for key in ('job_id', 'segment_number', 'start_ms', 'end_ms')}
                    result.update(text=text, model_version=model, processing_seconds=elapsed)
                    row = await run_blocking(save_result, result)
                    try:
                        LATENCY.labels(model=model).observe(elapsed)
                        REALTIME_FACTOR.labels(model=model).observe(elapsed / payload['audio_seconds'])
                        RESULTS.labels(result='success', model=model).inc()
                    except Exception:
                        LOGGER.exception('Unable to record transcription metrics')
            finally:
                try:
                    IN_PROGRESS.dec()
                except Exception:
                    LOGGER.exception('Unable to record completed work')
        await publish(channel, 'segment.transcribed', event(
            'segment.transcribed', **{key: row[key] for key in RESULT_FIELDS}))
        await message.ack()
    except asyncio.CancelledError:
        if not message.processed:
            await message.nack(requeue=True)
        raise
    except Exception:
        LOGGER.exception('Message processing failed; returning delivery to the queue')
        if not message.processed:
            await asyncio.sleep(1)  # Avoid a hot requeue loop during an outage.
            await message.nack(requeue=True)


async def main() -> None:
    if CONCURRENCY < 1 or not math.isfinite(TIMEOUT_SECONDS) or TIMEOUT_SECONDS <= 0:
        raise ValueError("Concurrency and timeout must be positive")
    await asyncio.to_thread(initialise_database)
    storage = await asyncio.to_thread(wait_for_storage)
    transcriber = Transcriber()
    start_http_server(METRICS_PORT)
    rabbit = await connect_with_retry()
    channel = await rabbit.channel(publisher_confirms=True, on_return_raises=True)
    await channel.set_qos(prefetch_count=CONCURRENCY)
    await declare_topology(channel)
    queue = await channel.get_queue("transcription.segments")
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async def callback(message: aio_pika.IncomingMessage) -> None:
        async with semaphore:
            await handle_message(message, channel, storage, transcriber)

    try:
        await queue.consume(callback)
        await asyncio.Future()
    finally:
        await rabbit.close()


if __name__ == "__main__":
    asyncio.run(main())
