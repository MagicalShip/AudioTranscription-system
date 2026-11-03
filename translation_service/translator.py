from __future__ import annotations

import os
from dataclasses import dataclass

import httpx


PROMPT_VERSION = "en-zh-v1"


@dataclass(frozen=True)
class Translation:
    text: str

class Translator:
    """Small adapter: deterministic simulation by default, optional local Ollama."""

    def __init__(self) -> None:
        self.provider = os.getenv("TRANSLATION_PROVIDER", "simulated")
        self.model = os.getenv("TRANSLATION_MODEL", "qwen2.5:3b")
        self.prompt_version = "not-applicable" if self.provider == "simulated" else PROMPT_VERSION

    def translate(self, text: str) -> Translation:
        if os.getenv("TRANSLATION_FORCE_FAILURE", "false").lower() == "true":
            raise RuntimeError("Injected translation failure")
        if self.provider == "simulated":
            # Predictable zero-cost fallback used for architecture demonstrations and tests.
            translated = f"[模拟中文翻译] {text}"
            return Translation(translated)

        raise ValueError(f"Unsupported TRANSLATION_PROVIDER: {self.provider}")

