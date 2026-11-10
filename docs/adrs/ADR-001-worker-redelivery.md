# ADR-001: Use at-least-once delivery with idempotent transcription results

## Status

Accepted

## Context

Three transcription consumers may process different segments concurrently. A consumer can crash after running the model but before acknowledging RabbitMQ. The assessed system must recover without losing the segment and without producing duplicate stored text. The team has four student developers and must keep the mechanism explainable and demonstrable locally.

This decision must be reopened if duplicate model execution costs more than one minute of CPU time per job, or if a future requirement demands externally visible side effects exactly once.

## Decision

Yokoi, Yuki will use RabbitMQ at-least-once delivery. A consumer acknowledges only after saving and publishing a result. The database key `(job_id, segment_number, model_version)` makes storing a redelivered result idempotent. If that key already exists, the worker republishes the saved result rather than rerunning the model.

## Consequences

- A crash before acknowledgement does not lose work, meeting the resilience requirement in a test that needs only one container kill.
- A crash before the database commit may repeat one model call. This is acceptable under the zero-money-cost constraint, but can consume up to one extra segment's CPU time.
- Delivery remains at least once, so downstream consumers also require idempotency. Job Service already provides a unique segment key and event inbox; this adds two small tables rather than an exactly-once coordinator.
- The design is simpler than distributed transactions, fitting a four-person student team, but duplicate events remain visible in RabbitMQ metrics.
