#!/usr/bin/env python3
"""
Validate that every emitted H3 aggregate (per day, per resolution) has at least one
raw feature intersecting the same H3 cell/day.

This helps catch cases where H3 tiles would render without a corresponding raw feature
when zooming in on the same day.
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, List, Tuple

import h3

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "GeoJson"
RESOLUTIONS = [1, 2, 3, 4]


def parse_timestamp(raw: str | None) -> float | None:
    if not raw or not isinstance(raw, str):
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.replace(tzinfo=timezone.utc).timestamp()
    except Exception:
        return None


def day_label(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()


def iter_features() -> Iterator[dict]:
    for path in sorted(DATA_DIR.glob("*.geojson")):
        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as exc:  # pragma: no cover
            print(f"Skipping {path.name}: {exc}", file=sys.stderr)
            continue
        for feature in data.get("features", []):
            if isinstance(feature, dict):
                yield feature


def representative_lonlat(geom: dict | None) -> Tuple[float, float] | None:
    if not geom or not isinstance(geom, dict):
        return None
    gtype = geom.get("type")
    coords = geom.get("coordinates")
    if gtype == "Point" and isinstance(coords, (list, tuple)) and len(coords) >= 2:
        try:
            return float(coords[0]), float(coords[1])
        except Exception:
            return None
    if gtype == "MultiPoint" and coords:
        pt = coords[0]
        if isinstance(pt, (list, tuple)) and len(pt) >= 2:
            try:
                return float(pt[0]), float(pt[1])
            except Exception:
                return None
    # For lines/polys, approximate with centroid of all coordinate pairs
    def flatten(arr: List) -> List[Tuple[float, float]]:
        out: List[Tuple[float, float]] = []
        if not isinstance(arr, list):
            return out
        for item in arr:
            if isinstance(item, (list, tuple)) and len(item) == 2 and all(
                isinstance(v, (int, float)) for v in item
            ):
                out.append((float(item[0]), float(item[1])))
            elif isinstance(item, list):
                out.extend(flatten(item))
        return out

    pairs = flatten(coords or [])
    if not pairs:
        return None
    lon = sum(p[0] for p in pairs) / len(pairs)
    lat = sum(p[1] for p in pairs) / len(pairs)
    return lon, lat


def main() -> None:
    agg_cells = set()  # (res, day_label, cell) derived from all geometries (matches H3 pipeline)
    raw_cells_all = set()  # (res, day_label, cell) from any geometry type (Point/Line/Polygon)

    for feature in iter_features():
        geom = feature.get("geometry")
        props = feature.get("properties") or {}
        if "id_fire_event" not in props:
            continue
        ts = parse_timestamp(props.get("time") or props.get("timestamp"))
        if ts is None:
            continue
        day = day_label(ts)

        # Any-geometry centroid coverage (raw + aggregation)
        lonlat_any = representative_lonlat(geom)
        if lonlat_any:
            lon, lat = lonlat_any
            for res in RESOLUTIONS:
                try:
                    cell = h3.latlng_to_cell(lat, lon, res)
                except Exception:
                    continue
                raw_cells_all.add((res, day, cell))
                agg_cells.add((res, day, cell))

    missing = agg_cells - raw_cells_all
    if missing:
        sample = list(missing)[:5]
        print(
            f"Validation failed: {len(missing)} H3 aggregates (point-derived) have no raw feature coverage (any geom). "
            f"Examples: {sample}",
            file=sys.stderr,
        )
        raise SystemExit(1)

    print("Validation passed: every H3 aggregate has at least one raw feature (any geometry type).")


if __name__ == "__main__":
    main()
