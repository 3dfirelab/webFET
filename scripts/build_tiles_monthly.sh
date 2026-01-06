#!/usr/bin/env bash
set -euo pipefail

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

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOT_DIR}/tiles"

MAX_ZOOM="${MAX_ZOOM:-11}"          # tippecanoe max zoom for raw layer
RAW_MIN_ZOOM="${RAW_MIN_ZOOM:-7}"   # tippecanoe min zoom for raw layer (shows at z≥7)
H3_MIN_ZOOM="${H3_MIN_ZOOM:-0}"     # tippecanoe min zoom for H3 layer
H3_RES="${H3_RES:-4}"
LOW_ZOOM_MAX="${LOW_ZOOM_MAX:-7}"
HIGH_ZOOM_MIN="${HIGH_ZOOM_MIN:-$((LOW_ZOOM_MAX + 1))}"

mkdir -p "${OUT_DIR}"

if ! command -v pmtiles >/dev/null 2>&1; then
  echo "ERROR: pmtiles CLI is required to produce .pmtiles outputs. Install from https://github.com/protomaps/PMTiles"
  exit 1
fi

# Default months if none supplied: derive from manifest.json
if [ "$#" -eq 0 ]; then
  MANIFEST="${ROOT_DIR}/GeoJson/manifest.json"
  if [ ! -f "${MANIFEST}" ]; then
    echo "ERROR: ${MANIFEST} not found and no months supplied."
    exit 1
  fi
  mapfile -t MONTHS < <(python3 - <<'PY' "${MANIFEST}"
import json, sys
from pathlib import Path
path = Path(sys.argv[1])
data = json.load(path.open())
months = sorted({(item.get("timestamp") or "")[:7] for item in data.get("items", []) if isinstance(item, dict) and (item.get("timestamp") or "").startswith("20")})
if not months:
    sys.exit("No months found in manifest")
print("\n".join(months))
PY
  )
else
  MONTHS=("$@")
fi

function next_month_iso() {
  python - "$1" <<'PY'
import sys
from datetime import date
m = sys.argv[1]
y, mo = map(int, m.split("-"))
if mo == 12:
    nxt = date(y + 1, 1, 1)
else:
    nxt = date(y, mo + 1, 1)
print(nxt.isoformat())
PY
}

for MONTH in "${MONTHS[@]}"; do
  START="${MONTH}-01"
  END="$(next_month_iso "${MONTH}")"
  OUT_H3_MB="${OUT_DIR}/fires_h3_${MONTH}.mbtiles"
  OUT_POINTS_MB="${OUT_DIR}/fires_points_${MONTH}.mbtiles"
  OUT_H3_PM="${OUT_DIR}/fires_h3_${MONTH}.pmtiles"
  OUT_POINTS_PM="${OUT_DIR}/fires_points_${MONTH}.pmtiles"

  rm -f "${OUT_H3_MB}" "${OUT_POINTS_MB}" "${OUT_H3_PM}" "${OUT_POINTS_PM}"

  echo "Building H3-only tiles for ${MONTH} -> ${OUT_H3_PM}"
  python "${ROOT_DIR}/scripts/stream_features_h3.py" \
    --h3-res "${H3_RES}" \
    --low-zoom-max "${LOW_ZOOM_MAX}" \
    --high-zoom-min "${HIGH_ZOOM_MIN}" \
    --omit-raw \
    --start-date "${START}" \
    --end-date "${END}" \
    | tippecanoe \
      -o "${OUT_H3_MB}" \
      -Z "${H3_MIN_ZOOM}" \
      -z "${LOW_ZOOM_MAX}" \
      --drop-densest-as-needed \
      --extend-zooms-if-still-dropping \
      -pk -pS -r1 \
      --force \
      --name="Fire events H3 ${MONTH}" \
      --layer=fires_h3 \
      /dev/stdin
  pmtiles convert "${OUT_H3_MB}" "${OUT_H3_PM}"
  rm -f "${OUT_H3_MB}"

  echo "Building raw-point tiles for ${MONTH} -> ${OUT_POINTS_PM}"
  python "${ROOT_DIR}/scripts/stream_features.py" \
    --start-date "${START}" \
    --end-date "${END}" \
    | tippecanoe \
      -o "${OUT_POINTS_MB}" \
      -Z "${RAW_MIN_ZOOM}" \
      -z "${MAX_ZOOM}" \
      --drop-densest-as-needed \
      --extend-zooms-if-still-dropping \
      -pk -pS -r1 \
      --force \
      --name="Fire events points ${MONTH}" \
      --layer=fires_points \
      /dev/stdin
  pmtiles convert "${OUT_POINTS_MB}" "${OUT_POINTS_PM}"
  rm -f "${OUT_POINTS_MB}"
done

echo "Monthly tiles built in ${OUT_DIR}"
