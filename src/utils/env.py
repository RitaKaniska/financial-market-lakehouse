from __future__ import annotations

import os
from pathlib import Path


def load_env_file(env_path: Path | str = Path(".env")) -> None:
    resolved_path = Path(env_path)
    if not resolved_path.exists():
        return

    for raw_line in resolved_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())
