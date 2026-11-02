# Architecture diagrams

## C4 system context

```mermaid
C4Context
  title System context
  Person(user, "User", "Uploads English audio and reads English/Chinese text")
  System(system, "Audio Transcription and Translation System", "Asynchronously segments, transcribes, reassembles and translates audio")
  Rel(user, system, "Uploads and checks jobs", "HTTP")
```

The assessed setup has no paid external system. Optional local Whisper and Ollama models run on the same machine.

## C4 containers

```mermaid
C4Container
  title Container view
  Person(user, "User")
  Container(job, "Job Service", "FastAPI/Python", "API, state owner, ordering and assembly")
  Container(audio, "Audio Service", "Python", "Validates and segments PCM WAV audio")
  Container(transcription, "Transcription Service", "Python/Whisper", "Parallel segment transcription")
  Container(translation, "Translation Service", "Python/Ollama", "English-to-Chinese translation")
  ContainerQueue(queue, "RabbitMQ", "Direct exchange", "Durable asynchronous work and events")
  ContainerDb(db, "PostgreSQL", "Database", "Job state, result and idempotency records")
  ContainerDb(objects, "Shared audio volume", "Docker volume", "Source audio and immutable segments")
  Container(monitoring, "Prometheus + Grafana", "Monitoring", "Metrics and dashboard")
  Rel(user, job, "Upload/query", "HTTP")
  Rel(job, queue, "Job and translation events", "AMQP")
  Rel(audio, queue, "Consume/publish", "AMQP")
  Rel(transcription, queue, "Consume/publish", "AMQP")
  Rel(translation, queue, "Consume/publish", "AMQP")
  Rel(job, db, "Own job state", "SQL")
  Rel(audio, db, "Segment manifest", "SQL")
  Rel(transcription, db, "Idempotent result", "SQL")
  Rel(translation, db, "Idempotent result", "SQL")
  Rel(job, objects, "Store source", "File reference")
  Rel(audio, objects, "Read source/write segments", "File reference")
  Rel(transcription, objects, "Read segments", "File reference")
  Rel(monitoring, job, "Scrape")
  Rel(monitoring, audio, "Scrape")
  Rel(monitoring, transcription, "Scrape")
  Rel(monitoring, translation, "Scrape")
```

## Processing sequence

```mermaid
sequenceDiagram
  actor User
  participant J as Job
  participant M as Shared volume
  participant Q as RabbitMQ
  participant A as Audio
  participant W as Transcription pool
  participant L as Translation
  User->>J: Upload
  J->>M: Store source once
  J->>Q: job.uploaded (outbox)
  Q->>A: job.uploaded
  A->>M: Write overlapping segments
  loop each segment
    A->>Q: segment.ready
    Q->>W: competing consumer
    W->>Q: segment.transcribed / segment.failed
    Q->>J: out-of-order outcome
  end
  J->>J: Order and remove duplicate boundary words
  J->>Q: transcript.completed
  Q->>L: transcript.completed
  L->>Q: translation.completed / failed
  Q->>J: final event
  User->>J: Poll job result
```
