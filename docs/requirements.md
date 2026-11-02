# Requirements and acceptance criteria

## Functional

1. Accept uncompressed PCM WAV English audio up to 30 minutes and 200 MB.
2. Show visible job state from upload through completion.
3. Split audio into 30-second segments with 0.5-second overlap.
4. Process segments concurrently and reassemble them in the original order.
5. Show a timestamped gap if a segment permanently fails; keep successful text.
6. Translate the completed English transcript to Simplified Chinese.
7. Keep the transcript available if translation fails, and allow translation retry.

## Non-functional acceptance criteria

| Requirement | Evidence |
|---|---|
| At least four deployable services | Four Dockerfiles and Compose services |
| Real asynchronous path | RabbitMQ events connect every processing stage |
| Worker redelivery | Kill transcription container; unacknowledged message returns |
| Idempotency | Primary key uses job ID + segment number + model version |
| Timeout and retry/backoff | AI calls time out; messages retry twice with exponential delay, then DLQ |
| Circuit breaker | Translation opens after three consecutive failures and resets after 30 s |
| Back-pressure | Queue buffers work; 20 active-job and 200 MB upload limits reject excess input clearly |
| Storage | A Docker shared volume stores audio; messages carry file references only |
| Consistency | Job Service owns authoritative state; processing events are eventually consistent |
| Observable | Grafana panels show rate, errors, latency, queue depth, processing and token metrics |
| Option B AI concerns | Version stamps, stored-first output, measured compute/tokens, prompt isolation |
| Zero paid cost | Default simulation; optional local Whisper and Ollama |
| One-command deployment | `docker compose up --build` |

## Deliberately out of scope

- Live microphone streaming
- More than 30 minutes per file
- Languages other than English to Simplified Chinese
- Speaker identification, accounts and permanent public hosting

These exclusions keep the project achievable while preserving the assessed distributed-systems decisions.
