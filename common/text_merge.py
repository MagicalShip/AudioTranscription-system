from __future__ import annotations

import re


def _tokens(text: str) -> list[str]:
    return re.findall(r"\S+", text.strip())


def merge_overlapping_text(parts: list[str], max_overlap_tokens: int = 20) -> str:
    """Merge ordered transcript parts and remove repeated boundary tokens."""
    merged: list[str] = []
    for part in parts:
        incoming = _tokens(part)
        if not incoming:
            continue
        overlap = 0
        limit = min(max_overlap_tokens, len(merged), len(incoming))
        for size in range(limit, 0, -1):
            left = [token.casefold().strip(".,!?;:") for token in merged[-size:]]
            right = [token.casefold().strip(".,!?;:") for token in incoming[:size]]
            if left == right:
                overlap = size
                break
        merged.extend(incoming[overlap:])
    return " ".join(merged)

