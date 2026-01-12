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
import re
import pyproj
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
    "time_floor",
}
ID_RE = re.compile(r"^gdf_(\d+)\.geojson$")
CRS_RE = re.compile(r"EPSG::?(\d+)")


def parse_timestamp(raw: str | None) -> tuple[str | None, float | None]:
    if not raw or not isinstance(raw, str):
        return None, None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        dt_utc = dt.replace(tzinfo=timezone.utc)
        return raw, dt_utc.timestamp()
    except Exception:
        return raw, None


def load_stats_map(path: Path | None) -> dict[str, dict]:
    if path is None or not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text())
    except Exception as exc:  # pragma: no cover
        print(f"Warning: failed to read stats file {path}: {exc}", file=sys.stderr)
        return {}
    mapping: dict[str, dict] = {}
    for feature in data.get("features", []):
        props = feature.get("properties") or {}
        fire_id = props.get("fire_event_id") or props.get("id_fire_event")
        if fire_id is None:
            continue
        start_iso, start_ts = parse_timestamp(props.get("time_start"))
        end_iso, end_ts = parse_timestamp(props.get("time_end"))
        mapping[str(fire_id)] = {
            "time_start": start_iso,
            "time_end": end_iso,
            "time_start_ts": start_ts,
            "time_end_ts": end_ts,
        }
    return mapping


def day_bounds(ts: float | None) -> tuple[float | None, float | None]:
    if ts is None:
        return None, None
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    start = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start.timestamp(), end.timestamp()


def build_transformer(crs_name: str | None) -> pyproj.Transformer | None:
    if not crs_name:
        return None
    match = CRS_RE.search(crs_name)
    if not match:
        return None
    epsg = int(match.group(1))
    if epsg == 4326:
        return None
    return pyproj.Transformer.from_crs(f"EPSG:{epsg}", "EPSG:4326", always_xy=True)


def transform_coords(coords, transformer: pyproj.Transformer):
    if isinstance(coords, (list, tuple)) and coords and isinstance(coords[0], (int, float)):
        x, y = coords[0], coords[1]
        x2, y2 = transformer.transform(x, y)
        tail = list(coords[2:]) if len(coords) > 2 else []
        return [x2, y2, *tail]
    if isinstance(coords, (list, tuple)):
        return [transform_coords(c, transformer) for c in coords]
    return coords


def transform_geometry(geom: dict, transformer: pyproj.Transformer) -> dict:
    coords = geom.get("coordinates")
    if coords is None:
        return geom
    return {**geom, "coordinates": transform_coords(coords, transformer)}


def compute_time_floor_range(data: dict) -> tuple[str | None, float | None, str | None, float | None]:
    min_ts = None
    max_ts = None
    for feat in data.get("features", []):
        props = feat.get("properties") or {}
        raw = props.get("time_floor") or props.get("time") or props.get("timestamp")
        _, ts = parse_timestamp(raw)
        if ts is None:
            continue
        if min_ts is None or ts < min_ts:
            min_ts = ts
        if max_ts is None or ts > max_ts:
            max_ts = ts
    if min_ts is None or max_ts is None:
        return None, None, None, None
    min_iso = datetime.fromtimestamp(min_ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    max_iso = datetime.fromtimestamp(max_ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    return min_iso, min_ts, max_iso, max_ts


def iter_features(data_dir: Path) -> Iterable[dict]:
    for path in sorted(data_dir.glob("*.geojson")):
        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as exc:  # pragma: no cover
            print(f"Skipping {path.name}: {exc}", file=sys.stderr)
            continue
        crs_name = None
        crs = data.get("crs") or {}
        if isinstance(crs, dict):
            props = crs.get("properties") or {}
            crs_name = props.get("name") or crs.get("name")
        elif isinstance(crs, str):
            crs_name = crs
        transformer = build_transformer(crs_name)
        file_id = None
        match = ID_RE.match(path.name)
        if match:
            file_id = match.group(1)
        min_iso, min_ts, max_iso, max_ts = compute_time_floor_range(data)
        for feature in data.get("features", []):
            if not isinstance(feature, dict):
                continue
            props = feature.get("properties") or {}
            if file_id and "id_fire_event" not in props:
                props = dict(props)
                props["id_fire_event"] = file_id
                if min_ts is not None and max_ts is not None:
                    props["time_min_ts"] = min_ts
                    props["time_max_ts"] = max_ts
                    props["time_min"] = min_iso
                    props["time_max"] = max_iso
                feature = dict(feature)
                feature["properties"] = props
            if transformer and isinstance(feature.get("geometry"), dict):
                feature = dict(feature)
                feature["geometry"] = transform_geometry(feature["geometry"], transformer)
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
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR, help="Directory of GeoJSON slices")
    parser.add_argument(
        "--stats-gdf",
        type=Path,
        default=None,
        help="Optional GeoJSON stats file with time_start/time_end per fire_event_id.",
    )
    args = parser.parse_args()
    start_ts = args.start_date
    end_ts = None
    if args.end_date is not None:
        end_dt = datetime.fromtimestamp(args.end_date, tz=timezone.utc) + timedelta(days=1)
        end_ts = end_dt.timestamp()

    stats_map = load_stats_map(args.stats_gdf)

    for feature in iter_features(args.data_dir):
        props = dict(feature.get("properties") or {})
        if "id_fire_event" not in props:
            continue
        minimal_props = {k: props[k] for k in ALLOWED_KEYS if k in props}
        minimal_props["id_fire_event"] = str(minimal_props.get("id_fire_event"))

        stats = stats_map.get(minimal_props["id_fire_event"])
        if stats and "time_min_ts" not in minimal_props:
            if stats.get("time_start_ts") is not None:
                minimal_props["time_min_ts"] = stats["time_start_ts"]
            if stats.get("time_end_ts") is not None:
                minimal_props["time_max_ts"] = stats["time_end_ts"]
            if stats.get("time_start"):
                minimal_props["time_min"] = stats["time_start"]
            if stats.get("time_end"):
                minimal_props["time_max"] = stats["time_end"]

        time_floor = minimal_props.get("time_floor")
        if time_floor:
            minimal_props["time"] = time_floor
        ts_str, ts_epoch = parse_timestamp(
            minimal_props.get("time_floor")
            or minimal_props.get("time")
            or minimal_props.get("timestamp")
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
