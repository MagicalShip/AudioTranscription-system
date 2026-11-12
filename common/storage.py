from __future__ import annotations

import shutil
from pathlib import Path

from common.config import STORAGE_ROOT


def wait_for_storage() -> Path:
    root = Path(STORAGE_ROOT)
    root.mkdir(parents=True, exist_ok=True)
    probe = root / ".write-test"
    probe.write_text("ok")
    probe.unlink()
    return root


def _safe_path(root: Path, object_name: str) -> Path:
    target = (root / object_name).resolve()
    if root.resolve() not in target.parents:
        raise ValueError("Invalid storage object name")
    return target


def upload_file(storage: Path, object_name: str, path: str | Path, content_type: str) -> None:
    del content_type  # Kept in the interface so an object store can be restored later.
    target = _safe_path(storage, object_name)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(path, target)


def download_file(storage: Path, object_name: str, path: str | Path) -> None:
    source = _safe_path(storage, object_name)
    shutil.copyfile(source, path)
