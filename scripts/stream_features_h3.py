#!/usr/bin/env python3
"""
Stream NDJSON that combines:
- An H3-binned layer for low zooms (default: z0–z4) using hex polygons.
- Raw features for higher zooms (default: z5+), tagged with a minzoom hint.

Usage (typically piped into tippecanoe):
  python3 scripts/stream_features_h3.py \\
    --h3-res 3 --low-zoom-max 4 --high-zoom-min 5 \\
    | tippecanoe -o tiles/fires.pmtiles -Z0 -z10 ...
"""
from __future__ import annotations

import argparse
import json
import sys
import re
import pyproj
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Tuple

import h3

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA_DIR = ROOT / "GeoJson"
ID_RE = re.compile(r"^gdf_(\d+)\.geojson$")
CRS_RE = re.compile(r"EPSG::?(\d+)")


@dataclass
class Aggregate:
    count: int = 0
    frp_sum: float = 0.0
    fre_sum: float = 0.0  # MJ over 10 min intervals, summed per fire id
    fre_mean_mj: float = 0.0  # derived later as average MJ per fire event
    frp_max: float = 0.0
    sample_time: str | None = None
    time_min: str | None = None
    time_max: str | None = None
    time_min_ts: float | None = None
    time_max_ts: float | None = None
    fros_sum: float = 0.0
    fros_max: float = 0.0
    fros_count: int = 0
    day_start_ts: float | None = None
    day_end_ts: float | None = None
    day_label: str | None = None
    res: int | None = None
    fire_ids: set = field(default_factory=set)


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


def compute_time_floor_range(data: dict) -> Tuple[str | None, float | None, str | None, float | None]:
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


def iter_features(data_dir: Path) -> Iterator[dict]:
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


def extract_lonlat(feature: dict) -> Tuple[float, float] | None:
    """Return lon/lat for any geometry; for non-points use centroid of coords."""
    geom = feature.get("geometry") or {}
    gtype = geom.get("type")
    coords = geom.get("coordinates")
    if gtype == "Point":
        if (
            not isinstance(coords, (list, tuple))
            or len(coords) < 2
            or coords[0] is None
            or coords[1] is None
        ):
            return None
        try:
            return float(coords[0]), float(coords[1])
        except Exception:
            return None

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


def parse_timestamp(raw: str | None) -> Tuple[str | None, float | None]:
    if not raw or not isinstance(raw, str):
        return None, None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        dt_utc = dt.replace(tzinfo=timezone.utc)
        return raw, dt_utc.timestamp()
    except Exception:
        return raw, None


def load_stats_map(path: Path | None) -> Dict[str, Dict]:
    if path is None or not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text())
    except Exception as exc:  # pragma: no cover
        print(f"Warning: failed to read stats file {path}: {exc}", file=sys.stderr)
        return {}
    mapping: Dict[str, Dict] = {}
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


def day_bucket(ts: float) -> Tuple[str, float, float]:
    """Return day label (YYYY-MM-DD) and start/end epoch seconds for UTC day."""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    start = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start.date().isoformat(), start.timestamp(), end.timestamp()


def h3_cell(lat: float, lon: float, res: int) -> str | None:
    """Compatibility wrapper for h3 lat/lon->cell across versions."""
    if hasattr(h3, "latlng_to_cell"):
        try:
            return h3.latlng_to_cell(lat, lon, res)  # type: ignore[attr-defined]
        except Exception:
            return None
    try:
        return h3.geo_to_h3(lat, lon, res)  # type: ignore[attr-defined]
    except Exception:
        return None


def normalize_fros(value) -> float | None:
    try:
        fros_val = float(value)
    except Exception:
        return None
    # -999 appears to be a sentinel for missing fros
    if fros_val <= -900:
        return None
    return fros_val


def add_tippecanoe_minzoom(feature: dict, minzoom: int, stats: Dict | None = None) -> dict:
    props = dict(feature.get("properties") or {})
    ts_str, ts_epoch = parse_timestamp(
        props.get("time_floor") or props.get("time") or props.get("timestamp")
    )
    if ts_epoch is not None:
        props["time_ts"] = ts_epoch
        if "time_floor" in props:
            props["time"] = props["time_floor"]
    if stats and "time_min_ts" not in props:
        if stats.get("time_start_ts") is not None:
            props["time_min_ts"] = stats["time_start_ts"]
        if stats.get("time_end_ts") is not None:
            props["time_max_ts"] = stats["time_end_ts"]
        if stats.get("time_start"):
            props["time_min"] = stats["time_start"]
        if stats.get("time_end"):
            props["time_max"] = stats["time_end"]
    props["tippecanoe"] = {"minzoom": minzoom}
    return {"type": "Feature", "properties": props, "geometry": feature.get("geometry")}


def build_h3_feature(cell: str, stats: Aggregate, max_zoom: int) -> dict:
    if hasattr(h3, "cell_to_boundary"):
        boundary = h3.cell_to_boundary(cell)  # type: ignore[attr-defined]
    else:
        boundary = h3.h3_to_geo_boundary(cell)  # type: ignore[attr-defined]
    ring = [[lng, lat] for lat, lng in boundary]
    # Ensure the polygon is closed
    if ring and ring[0] != ring[-1]:
        ring.append(ring[0])

    avg = stats.frp_sum / stats.count if stats.count else 0.0
    fre_mean_mj = stats.fre_sum / stats.count if stats.count else 0.0
    fros_avg = stats.fros_sum / stats.fros_count if stats.fros_count else None
    # For time-filtered H3, we keep a single time slice per aggregate (time_min/max identical)
    time_min = stats.time_min
    time_max = stats.time_max
    time_min_ts = stats.time_min_ts
    time_max_ts = stats.time_max_ts
    return {
        "type": "Feature",
        "properties": {
            "cell": cell,
            "res": stats.res,
            "count": stats.count,
            "frp_sum": round(stats.frp_sum, 3),
            "frp_max": round(stats.frp_max, 3),
            "frp_avg": round(avg, 3),
            "fre_sum_mj": round(stats.fre_sum, 3),
            "fre_mean_mj": round(fre_mean_mj, 3),
            "last_time": stats.sample_time,
            "time_min": time_min,
            "time_max": time_max,
            "time_min_ts": time_min_ts,
            "time_max_ts": time_max_ts,
            "day_start_ts": stats.day_start_ts,
            "day_end_ts": stats.day_end_ts,
            "day_label": stats.day_label,
            "fros_sum": round(stats.fros_sum, 3),
            "fros_max": round(stats.fros_max, 3),
            "fros_avg": round(fros_avg, 3) if fros_avg is not None else None,
            "tippecanoe": {"minzoom": 0, "maxzoom": max_zoom},
        },
        "geometry": {"type": "Polygon", "coordinates": [ring]},
    }


def stream(
    data_dir: Path,
    h3_res: int,
    low_zoom_max: int,
    high_zoom_min: int,
    include_raw: bool,
    start_ts: float | None = None,
    end_ts: float | None = None,
    stats_map: Dict[str, Dict] | None = None,
) -> Iterable[dict]:
    # Key H3 aggregates by (resolution, cell, day_start_ts) so FRP sums stay per day slice
    summary: Dict[Tuple[int, str, float], Aggregate] = defaultdict(Aggregate)
    resolutions = [4]  # only resolution 4

    for feature in iter_features(data_dir):
        lonlat = extract_lonlat(feature)
        props = feature.get("properties") or {}
        fire_id = props.get("id_fire_event")
        if fire_id is None:
            continue
        fire_id = str(fire_id)
        frp = float(props.get("frp") or 0.0)
        # FRP is MW; FRE (MJ) over 10 minutes (600s) => frp * 600
        fre = frp * 600.0
        timestamp_raw = props.get("time_floor") or props.get("time") or props.get("timestamp")
        ts_str, ts_epoch = parse_timestamp(timestamp_raw)
        if ts_epoch is not None:
            if start_ts is not None and ts_epoch < start_ts:
                continue
            if end_ts is not None and ts_epoch >= end_ts:
                continue
        fros_val = normalize_fros(props.get("fros"))

        if lonlat and ts_epoch is not None:
            lon, lat = lonlat
            day_label, day_start_ts, day_end_ts = day_bucket(ts_epoch)
            for res in resolutions:
                cell = h3_cell(lat, lon, res)
                if not cell:
                    continue
                key = (res, cell, day_start_ts)
                agg = summary[key]
                agg.res = res
                agg.day_start_ts = day_start_ts
                agg.day_end_ts = day_end_ts
                agg.day_label = day_label
                if fire_id not in agg.fire_ids:
                    agg.fire_ids.add(fire_id)
                    agg.count += 1
                    agg.frp_sum += frp
                    agg.fre_sum += fre
                    if fros_val is not None:
                        agg.fros_sum += fros_val
                        agg.fros_max = max(agg.fros_max, fros_val)
                        agg.fros_count += 1
                agg.frp_max = max(agg.frp_max, frp)
                agg.sample_time = ts_str or agg.sample_time
                agg.time_min = day_label if agg.time_min is None else agg.time_min
                agg.time_max = day_label if agg.time_max is None else agg.time_max
                agg.time_min_ts = day_start_ts if agg.time_min_ts is None else agg.time_min_ts
                agg.time_max_ts = day_end_ts if agg.time_max_ts is None else agg.time_max_ts

        if include_raw:
            stats = stats_map.get(fire_id) if stats_map else None
            yield add_tippecanoe_minzoom(feature, high_zoom_min, stats)

    for (res, cell, _), stats in summary.items():
        yield build_h3_feature(cell, stats, low_zoom_max)


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream raw and H3-aggregated NDJSON features.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="Directory of GeoJSON slices")
    parser.add_argument("--h3-res", type=int, default=3, help="H3 resolution for low zooms (default: 3)")
    parser.add_argument("--low-zoom-max", type=int, default=4, help="Max zoom to show H3 layer (default: 4)")
    parser.add_argument(
        "--high-zoom-min",
        type=int,
        default=None,
        help="Min zoom to show raw features (default: low-zoom-max + 1)",
    )
    parser.add_argument(
        "--omit-raw",
        action="store_true",
        help="Do not emit raw point features (only H3 aggregates)",
    )
    def parse_date(value: str | None) -> float | None:
        if not value:
            return None
        try:
            d = date.fromisoformat(value)
            return datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp()
        except Exception:
            raise argparse.ArgumentTypeError(f"Invalid date: {value}")

    parser.add_argument("--start-date", type=parse_date, default=None, help="ISO date YYYY-MM-DD inclusive (UTC)")
    parser.add_argument(
        "--end-date",
        type=parse_date,
        default=None,
        help="ISO date YYYY-MM-DD inclusive (UTC). Internally uses next-day boundary.",
    )
    parser.add_argument(
        "--stats-gdf",
        type=Path,
        default=None,
        help="Optional GeoJSON stats file with time_start/time_end per fire_event_id.",
    )

    args = parser.parse_args()

    high_zoom_min = args.high_zoom_min if args.high_zoom_min is not None else args.low_zoom_max + 1
    start_ts = args.start_date
    end_ts = None
    if args.end_date is not None:
        end_dt = datetime.fromtimestamp(args.end_date, tz=timezone.utc) + timedelta(days=1)
        end_ts = end_dt.timestamp()

    stats_map = load_stats_map(args.stats_gdf)

    try:
        for feature in stream(
            args.data_dir,
            args.h3_res,
            args.low_zoom_max,
            high_zoom_min,
            include_raw=not args.omit_raw,
            start_ts=start_ts,
            end_ts=end_ts,
            stats_map=stats_map,
        ):
            sys.stdout.write(json.dumps(feature, separators=(",", ":")))
            sys.stdout.write("\n")
    except BrokenPipeError:
        # Allow callers to pipe into head/tee without noisy tracebacks
        pass


if __name__ == "__main__":
    main()
