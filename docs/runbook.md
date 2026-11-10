# Runbook

## Start and observe

Run `docker compose up --build`. A healthy stack exposes the app on port 8000, Grafana on 3000 and RabbitMQ management on 15672. Upload a file in the app and keep these views open:

- App: state and user-visible degraded results.
- Grafana `Audio Pipeline`: rate, errors, latency, queue depth, AI processing and token metrics.
- RabbitMQ `Queues`: queued, unacknowledged, retried and dead-lettered work.
- `docker compose logs -f SERVICE`: individual service evidence.

## Break and recover

### Transcription worker crash

1. Upload the generated 65-second sample.
2. Run `docker compose kill transcription-service` during transcription.
3. Run `docker compose up -d transcription-service`.
4. Verify the job completes and each segment number occurs once in `GET /jobs/{id}/segments`.

Expected: RabbitMQ redelivers unacknowledged work; the database key prevents duplicate stored results.

### Permanent segment failure

Start with `FAIL_SEGMENT_NUMBER=1 docker compose up --build`, upload the sample and wait through retries.

Expected: three attempts, a message in `transcription.segments.dlq`, a timestamped gap, and `PARTLY_DONE`.

### Translation outage

Start with `TRANSLATION_FORCE_FAILURE=true docker compose up --build` and upload the sample.

Expected: English transcript remains, status is `PARTLY_DONE`, retries/dead letter are visible, and the circuit metric becomes 1. Restart without the flag and call `POST /jobs/{id}/retry-translation`.

### Back-pressure

Temporarily set `MAX_ACTIVE_JOBS=1`, keep one task processing, and submit another.

Expected: HTTP 429 and `The system is busy. Please try again later.` Existing work is not dropped.

## Stop and clean

`docker compose down` keeps data. `docker compose down -v` removes the project database and stored audio and cannot be undone.
