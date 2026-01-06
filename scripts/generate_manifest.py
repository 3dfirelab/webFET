#!/usr/bin/env python3
"""
Create a lightweight manifest of all GeoJSON time slices so the Leaflet viewer
can load them on demand without scanning the directory from the browser.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import List, TypedDict


class Entry(TypedDict):
    file: str
    timestamp: str
    label: str


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "GeoJson"
MANIFEST_PATH = DATA_DIR / "manifest.json"
FILENAME_RE = re.compile(r"^firEvents-(\d{4}-\d{2}-\d{2})_(\d{4})\.geojson$")


def main() -> None:
    if not DATA_DIR.is_dir():
        raise SystemExit(f"Data directory not found: {DATA_DIR}")

    entries: List[Entry] = []
    for path in sorted(DATA_DIR.glob("firEvents-*.geojson")):
        match = FILENAME_RE.match(path.name)
        if not match:
            continue
        date_part, time_part = match.groups()
        hours, minutes = time_part[:2], time_part[2:]
        iso_time = f"{date_part}T{hours}:{minutes}:00Z"
        label = f"{date_part} {hours}:{minutes}"
        entries.append({"file": path.name, "timestamp": iso_time, "label": label})

    if not entries:
        raise SystemExit("No matching GeoJSON files were found.")

    payload = {
        "generatedAt": datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
        "count": len(entries),
        "items": entries,
    }
    MANIFEST_PATH.write_text(json.dumps(payload, indent=2))
    print(f"Wrote {len(entries)} entries to {MANIFEST_PATH}")


if __name__ == "__main__":
    main()
