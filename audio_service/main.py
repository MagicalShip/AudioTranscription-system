from __future__ import annotations

import asyncio
import math
import os
import tempfile
import time
import wave
from pathlib import Path
from typing import Any

import aio_pika
from prometheus_client import Counter, Histogram, start_http_server

from common.database import connection, wait_for_postgres
from common.events import event
from common.messaging import (
    connect_with_retry,
    decode,
    declare_topology,
    publish,
    retry_or_dead_letter,
)
from common.storage import download_file, upload_file, wait_for_storage


MAX_AUDIO_SECONDS = int(os.getenv("MAX_AUDIO_SECONDS", "1800"))
SEGMENT_SECONDS = int(os.getenv("SEGMENT_SECONDS", "30"))
OVERLAP_SECONDS = float(os.getenv("OVERLAP_SECONDS", "0.5"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

JOBS = Counter("audio_jobs_total", "Audio jobs processed", ["result"])
SEGMENTS = Counter("audio_segments_created_total", "Audio segments created")
SEGMENTATION_TIME = Histogram("audio_segmentation_duration_seconds", "Time spent segmenting an upload")
AUDIO_DURATION = Histogram("uploaded_audio_duration_seconds", "Duration of uploaded audio")


def initialise_database() -> None:
    wait_for_postgres()
    schema = Path(__file__).with_name("schema.sql").read_text()
    with connection() as conn:
        conn.execute(schema)
        conn.commit()


def probe_duration(path: Path) -> float:
    with wave.open(str(path), "rb") as audio:
        return audio.getnframes() / audio.getframerate()


def split_audio(source: Path, output: Path, start: float, duration: float) -> None:
    with wave.open(str(source), "rb") as audio:
        frame_rate = audio.getframerate()
        audio.setpos(min(round(start * frame_rate), audio.getnframes()))
        frames = audio.readframes(round(duration * frame_rate))
        with wave.open(str(output), "wb") as segment:
            segment.setparams(audio.getparams())
            segment.writeframes(frames)


def existing_segments(job_id: str) -> list[dict[str, Any]]:
    with connection() as conn:
        job = conn.execute("SELECT * FROM audio_jobs WHERE job_id=%s", (job_id,)).fetchone()
        if not job or job["status"] != "DONE":
            return []
        rows = conn.execute(
            "SELECT * FROM audio_segments WHERE job_id=%s ORDER BY segment_number",
            (job_id,),
        ).fetchall()
        return [dict(row) for row in rows]


def save_segment(row: dict[str, Any]) -> None:
    with connection() as conn:
        conn.execute(
            """
            INSERT INTO audio_segments
                (job_id, segment_number, object_name, start_ms, end_ms, audio_seconds)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (job_id, segment_number) DO UPDATE SET
                object_name=EXCLUDED.object_name, start_ms=EXCLUDED.start_ms,
                end_ms=EXCLUDED.end_ms, audio_seconds=EXCLUDED.audio_seconds
            """,
            (
                row["job_id"],
                row["segment_number"],
                row["object_name"],
                row["start_ms"],
                row["end_ms"],
                row["audio_seconds"],
            ),
        )
        conn.commit()


def mark_audio_job(job_id: str, source: str, status: str, duration: float, total: int) -> None:
    with connection() as conn:
        conn.execute(
            """
            INSERT INTO audio_jobs (job_id, source_object, status, duration_seconds, total_segments)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (job_id) DO UPDATE SET status=EXCLUDED.status,
                duration_seconds=EXCLUDED.duration_seconds, total_segments=EXCLUDED.total_segments,
                updated_at=NOW()
            """,
            (job_id, source, status, duration, total),
        )
        conn.commit()


async def publish_segments(channel, job_id: str, rows: list[dict[str, Any]]) -> None:
    for row in rows:
        payload = event(
            "segment.ready",
            job_id=job_id,
            segment_number=row["segment_number"],
            object_name=row["object_name"],
            start_ms=row["start_ms"],
            end_ms=row["end_ms"],
            audio_seconds=row["audio_seconds"],
        )
        await publish(channel, "segment.ready", payload)
    await publish(
        channel,
        "segmentation.completed",
        event("segmentation.completed", job_id=job_id, total_segments=len(rows)),
    )


def create_segments(storage, payload: dict[str, Any]) -> list[dict[str, Any]]:
    job_id = payload["job_id"]
    cached = existing_segments(job_id)
    if cached:
        return cached

    suffix = Path(payload["filename"]).suffix or ".audio"
    with tempfile.TemporaryDirectory() as temp_dir:
        temp = Path(temp_dir)
        source = temp / f"source{suffix}"
        download_file(storage, payload["object_name"], source)
        duration = probe_duration(source)
        if duration <= 0:
            raise ValueError("Audio duration is zero")
        if duration > MAX_AUDIO_SECONDS:
            raise ValueError(f"Audio exceeds the {MAX_AUDIO_SECONDS // 60}-minute limit")
        AUDIO_DURATION.observe(duration)
        total = math.ceil(duration / SEGMENT_SECONDS)
        mark_audio_job(job_id, payload["object_name"], "PROCESSING", duration, total)
        rows: list[dict[str, Any]] = []
        for number in range(total):
            nominal_start = number * SEGMENT_SECONDS
            start = max(0.0, nominal_start - (OVERLAP_SECONDS if number else 0.0))
            end = min(duration, (number + 1) * SEGMENT_SECONDS + OVERLAP_SECONDS)
            segment_path = temp / f"segment-{number:04d}.wav"
            split_audio(source, segment_path, start, end - start)
            object_name = f"segments/{job_id}/{number:04d}.wav"
            upload_file(storage, object_name, segment_path, "audio/wav")
            row = {
                "job_id": job_id,
                "segment_number": number,
                "object_name": object_name,
                "start_ms": round(start * 1000),
                "end_ms": round(end * 1000),
                "audio_seconds": end - start,
            }
            save_segment(row)
            rows.append(row)
            SEGMENTS.inc()
        mark_audio_job(job_id, payload["object_name"], "DONE", duration, total)
        return rows


async def main() -> None:
    await asyncio.to_thread(initialise_database)
    storage = await asyncio.to_thread(wait_for_storage)
    start_http_server(METRICS_PORT)
    rabbit = await connect_with_retry()
    channel = await rabbit.channel()
    await channel.set_qos(prefetch_count=1)
    await declare_topology(channel)
    queue = await channel.get_queue("audio.jobs")

    async def callback(message: aio_pika.IncomingMessage) -> None:
        payload = decode(message)
        start = time.perf_counter()
        try:
            rows = await asyncio.to_thread(create_segments, storage, payload)
            await publish_segments(channel, payload["job_id"], rows)
            JOBS.labels(result="success").inc()
            SEGMENTATION_TIME.observe(time.perf_counter() - start)
            await message.ack()
        except Exception as exc:
            retried = await retry_or_dead_letter(channel, message, "job.uploaded", payload)
            if not retried:
                JOBS.labels(result="failed").inc()
                await publish(
                    channel,
                    "pipeline.failed",
                    event(
                        "pipeline.failed",
                        job_id=payload["job_id"],
                        stage="AUDIO",
                        error=str(exc),
                    ),
                )
            await message.ack()

    await queue.consume(callback)
    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
