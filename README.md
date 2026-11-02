# Audio Transcription and Translation System

Build an Option B distributed system that accepts an English audio file, transcribes its segments in parallel, reassembles the transcript, and translates it into Simplified Chinese.

## Service ownership

- Job Service: upload, job state, result assembly and user API
- Audio Service: validation, segmentation and shared storage
- Transcription Service: parallel Whisper workers, retries and idempotency
- Translation Service: English-to-Chinese translation, resilience and AI metrics

See `docs/requirements.md`, `docs/proposal.md`, and `docs/architecture.md` before implementation.
