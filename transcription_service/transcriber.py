from __future__ import annotations

import os
from pathlib import Path


class Transcriber:
    def __init__(self) -> None:
        self.provider = os.getenv("TRANSCRIPTION_PROVIDER", "simulated")
        self.model_name = os.getenv("WHISPER_MODEL", "base")
        self._model = None

    @property
    def model_version(self) -> str:
        if self.provider == "faster_whisper":
            return f"faster-whisper:{self.model_name}"
        return "simulated-whisper:1.0"

    def transcribe(self, path: Path, segment_number: int) -> str:
        fail_number = os.getenv("FAIL_SEGMENT_NUMBER")
        if fail_number and segment_number == int(fail_number):
            raise RuntimeError("Injected transcription failure")
        if self.provider == "simulated":
            # Deterministic offline fallback required by the brief. It tests the
            # distributed workflow without pretending to be real transcription.
            return f"Simulated English transcript for segment {segment_number}."
        if self.provider != "faster_whisper":
            raise ValueError(f"Unknown transcription provider: {self.provider}")
        if self._model is None:
            try:
                from faster_whisper import WhisperModel
            except ImportError as exc:
                raise RuntimeError(
                    "faster-whisper is not installed; build with INSTALL_LOCAL_AI=true"
                ) from exc
            self._model = WhisperModel(
                self.model_name,
                device=os.getenv("WHISPER_DEVICE", "cpu"),
                compute_type=os.getenv("WHISPER_COMPUTE_TYPE", "int8"),
            )
        segments, _ = self._model.transcribe(
            str(path),
            language="en",
            beam_size=1,
            vad_filter=True,
            condition_on_previous_text=False,
        )
        return " ".join(segment.text.strip() for segment in segments).strip()
