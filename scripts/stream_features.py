#!/usr/bin/env python3
"""
Stream all GeoJSON features from GeoJson/*.geojson to stdout as NDJSON.
Designed to be piped into tippecanoe, e.g.:

  python3 scripts/stream_features.py | tippecanoe -o tiles/fires.pmtiles \
      -zg --drop-densest-as-needed --extend-zooms-if-still-dropping \
      -pk -pS -r1 --force --name="Fire events" --layer=fires -
"""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta, date
import sys
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "GeoJson"
ALLOWED_KEYS = {
    "id_fire_event",
    "frp",
    "fros",
    "duration",
    "time",
    "timestamp",
}


def parse_timestamp(raw: str | None) -> tuple[str | None, float | None]:
    if not raw or not isinstance(raw, str):
        return None, None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        dt_utc = dt.replace(tzinfo=timezone.utc)
        return raw, dt_utc.timestamp()
    except Exception:
        return raw, None


def day_bounds(ts: float | None) -> tuple[float | None, float | None]:
    if ts is None:
        return None, None
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    start = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start.timestamp(), end.timestamp()


def iter_features() -> Iterable[dict]:
    for path in sorted(DATA_DIR.glob("*.geojson")):
        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as exc:  # pragma: no cover
            print(f"Skipping {path.name}: {exc}", file=sys.stderr)
            continue
        for feature in data.get("features", []):
            if not isinstance(feature, dict):
                continue
            yield feature


def main() -> None:
    import argparse

    def parse_date(value: str | None) -> float | None:
        if not value:
            return None
        try:
            d = date.fromisoformat(value)
            return datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp()
        except Exception:
            raise argparse.ArgumentTypeError(f"Invalid date: {value}")

    parser = argparse.ArgumentParser(description="Stream GeoJSON features as NDJSON (tippecanoe input).")
    parser.add_argument("--start-date", type=parse_date, default=None, help="ISO date YYYY-MM-DD inclusive (UTC)")
    parser.add_argument(
        "--end-date",
        type=parse_date,
        default=None,
        help="ISO date YYYY-MM-DD inclusive (UTC). Internally uses next-day boundary.",
    )
    args = parser.parse_args()
    start_ts = args.start_date
    end_ts = None
    if args.end_date is not None:
        end_dt = datetime.fromtimestamp(args.end_date, tz=timezone.utc) + timedelta(days=1)
        end_ts = end_dt.timestamp()

    for feature in iter_features():
        props = dict(feature.get("properties") or {})
        if "id_fire_event" not in props:
            continue
        minimal_props = {k: props[k] for k in ALLOWED_KEYS if k in props}
        minimal_props["id_fire_event"] = str(minimal_props.get("id_fire_event"))

        ts_str, ts_epoch = parse_timestamp(
            minimal_props.get("time") or minimal_props.get("timestamp")
        )
        if ts_epoch is not None:
            if start_ts is not None and ts_epoch < start_ts:
                continue
            if end_ts is not None and ts_epoch >= end_ts:
                continue
        if ts_epoch is not None:
            minimal_props["time_ts"] = ts_epoch
            day_start, day_end = day_bounds(ts_epoch)
            minimal_props["day_start_ts"] = day_start
            minimal_props["day_end_ts"] = day_end
        feature_out = {
            "type": "Feature",
            "properties": minimal_props,
            "geometry": feature.get("geometry"),
        }
        sys.stdout.write(json.dumps(feature_out, separators=(",", ":")))
        sys.stdout.write("\n")


if __name__ == "__main__":
    main()
