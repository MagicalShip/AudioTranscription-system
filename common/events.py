from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


def event(event_type: str, **payload: Any) -> dict[str, Any]:
    return {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        **payload,
    }

