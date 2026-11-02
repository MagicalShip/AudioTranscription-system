# Group 12 Project Proposal

**Option:** Option B - AI-native distributed system

**System:** A file-based English audio transcription and Chinese translation tool. Users upload audio of up to 30 minutes, view job progress, and receive an ordered English transcript followed by a Simplified Chinese translation. Local or simulated models keep the system at zero cost.

**Services:**

1. **Job Service - Cao, Mingshu:** upload API and UI, job state, result ordering, overlap removal and final result.
2. **Audio Service - Zhang, Zhexi:** validates duration, stores audio on a shared volume, creates overlapping segments and publishes references.
3. **Transcription Service - Yokoi, Yuki:** a worker pool processes segments in parallel, retries failures and stores idempotent results.
4. **Translation Service - Liu, Yingqi:** translates the completed transcript from English to Chinese with timeout, retry and circuit breaker.

RabbitMQ provides the asynchronous pipeline, PostgreSQL stores job and idempotency state, a Docker shared volume stores audio, and Prometheus/Grafana provide metrics. Results record model and prompt versions, processing time and token counts.

**Riskiest architectural decision:** transcription workers finish out of order and can crash mid-segment. Job Service reassembles by segment number. RabbitMQ redelivers unacknowledged messages, while the unique key `job_id + segment_number + model_version` prevents duplicate results. After bounded retries, a failed segment becomes a clearly labelled timestamped gap and the user receives a `PARTLY_DONE` result rather than losing the whole file.
