#!/usr/bin/env bash
set -euo pipefail

# Rebuild vector mask tiles (mask and 1 km buffered mask) from the NetCDF mask.
# Usage: ./scripts/build_mask_tiles.sh [source_nc] [tiles_dir]

activate_env() {
  local host
  host="$(hostname -s 2>/dev/null || hostname)"
  if [[ "$host" == "pc70852" ]] && [[ -f ~/miniforge3/bin/activate ]]; then
    source ~/miniforge3/bin/activate fci
    return
  fi
  if { [[ "$host" == "estella" ]] || [[ "$host" == "estrella" ]]; } && [[ -f ~/miniconda3/bin/activate ]]; then
    source ~/miniconda3/bin/activate fci
    return
  fi
  if [[ -f ~/miniforge3/bin/activate ]]; then
    source ~/miniforge3/bin/activate fci
    return
  fi
  if [[ -f ~/miniconda3/bin/activate ]]; then
    source ~/miniconda3/bin/activate fci
    return
  fi
  echo "WARNING: Could not find conda activate script; proceeding without activating 'fci' env." >&2
}

activate_env
export PATH='/home/paugam/Installed_Lib/PMTILES/':$PATH

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT_MASK="$(cd "$(dirname "${BASH_SOURCE[0]}")/../Mask_HS/" && pwd)"
SRC="${1:-"$ROOT_MASK/hs_density_mask_med.nc"}"
TILES_DIR="${2:-"$ROOT/tiles"}"

RAW_GEOJSON="$ROOT_MASK/data_mask_raw.geojson"
MASK_GEOJSON="$ROOT/data_mask.geojson"
MASK_MERC="$ROOT/data_mask_3857.geojson"
BUFFER_MERC="$ROOT/data_mask_buffer_3857.geojson"
BUFFER_GEOJSON="$ROOT/data_mask_buffer.geojson"
MASK_MB="$TILES_DIR/hs_mask.mbtiles"
BUFFER_MB="$TILES_DIR/hs_mask_buffer.mbtiles"
MASK_PMTILES="$TILES_DIR/hs_mask.pmtiles"
BUFFER_PMTILES="$TILES_DIR/hs_mask_buffer.pmtiles"

GDAL_POLY_BIN="${GDAL_POLY_BIN:-$(command -v gdal_polygonize.py || true)}"
if [[ -z "$GDAL_POLY_BIN" ]]; then
  echo "gdal_polygonize.py not found on PATH. Install GDAL (gdal-bin) or set GDAL_POLY_BIN." >&2
  exit 1
fi

if ! command -v pmtiles >/dev/null 2>&1; then
  echo "ERROR: pmtiles CLI is required to produce .pmtiles outputs. Install from https://github.com/protomaps/PMTiles" >&2
  exit 1
fi

if [[ ! -f "$SRC" ]]; then
  echo "Source mask not found: $SRC" >&2
  exit 1
fi

mkdir -p "$TILES_DIR"

echo "Polygonizing mask from $SRC..."
"$GDAL_POLY_BIN" "$SRC" -b 1 -f GeoJSON "$RAW_GEOJSON" mask DN

echo "Filtering mask=1..."
ogr2ogr -f GeoJSON "$MASK_GEOJSON" "$RAW_GEOJSON" -where "DN=1"

echo "Reprojecting to EPSG:3857 for buffering..."
ogr2ogr -t_srs EPSG:3857 "$MASK_MERC" "$MASK_GEOJSON"

echo "Buffering 1 km..."
rm -f "$BUFFER_MERC" "$BUFFER_GEOJSON"
ogr2ogr -f GeoJSON "$BUFFER_MERC" "$MASK_MERC" \
  -dialect sqlite -sql "SELECT ST_Buffer(geometry,1000) AS geometry, * FROM mask"

echo "Reprojecting buffered mask back to EPSG:4326..."
ogr2ogr -t_srs EPSG:4326 "$BUFFER_GEOJSON" "$BUFFER_MERC"

echo "Building pmtiles (mask)..."
rm -f "$MASK_MB" "$MASK_PMTILES"
tippecanoe -Z7 -z14 -o "$MASK_MB" -l mask --force "$MASK_GEOJSON"
pmtiles convert "$MASK_MB" "$MASK_PMTILES"
rm -f "$MASK_MB"

echo "Building pmtiles (mask + 1 km buffer)..."
rm -f "$BUFFER_MB" "$BUFFER_PMTILES"
tippecanoe -Z7 -z14 -o "$BUFFER_MB" -l mask_buffer --force "$BUFFER_GEOJSON"
pmtiles convert "$BUFFER_MB" "$BUFFER_PMTILES"
rm -f "$BUFFER_MB"

echo "Done."

# Quick sanity check for PMTiles magic header
for f in "$MASK_PMTILES" "$BUFFER_PMTILES"; do
  if [[ -f "$f" ]]; then
    magic=$(head -c 7 "$f")
    if [[ "$magic" != "PMTiles" ]]; then
      echo "ERROR: $f does not start with PMTiles magic; file may be corrupt." >&2
      exit 1
    fi
  fi
done
