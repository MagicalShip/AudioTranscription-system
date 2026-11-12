# ADR-003: Pass object references instead of audio through RabbitMQ

## Status

Accepted

## Context

An accepted upload can be 200 MB and its WAV segments can each be roughly 1 MB. RabbitMQ is needed to schedule work, not to serve as a large-file store. The system must run on one laptop and multiple workers must read the same immutable segment.

This decision must be reopened if deployment cannot provide storage shared by every worker, or if measured object-store access becomes the main contributor to processing time.

## Decision

Zhang, Zhexi will store each source and segment once on a Docker shared volume. RabbitMQ messages will contain the relative file reference, job ID, segment number and timing metadata only.

## Consequences

- Queue messages remain a few hundred bytes rather than up to 200 MB, keeping RabbitMQ responsive during several uploads.
- Workers can retry and redownload the same immutable object, supporting at-least-once processing.
- The shared volume becomes an infrastructure dependency and uses local disk. A 200 MB upload plus uncompressed segments can temporarily consume roughly another 60 MB per 30 minutes; test data must therefore be cleaned after demonstrations.
- Audio has one authoritative copy, but deleting jobs now requires later object cleanup. Automatic retention is outside the first assessed slice and must be documented operationally.
