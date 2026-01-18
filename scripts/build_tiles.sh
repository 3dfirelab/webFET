#!/usr/bin/env bash
set -euo pipefail

activate_env() {
  local host
  host="$(hostname -s 2>/dev/null || hostname)"
  if [[ "$host" == "pc70852" ]] && [[ -f ~/miniforge3/bin/activate ]]; then
    # Local workstation
    source ~/miniforge3/bin/activate fci
    return
  fi
  if { [[ "$host" == "estella" ]] || [[ "$host" == "estrella" ]]; } && [[ -f ~/miniconda3/bin/activate ]]; then
    # Remote host
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
OUT_H3_PM="${OUT_DIR}/fires_h3.pmtiles"
OUT_POINTS_PM="${OUT_DIR}/fires_points.pmtiles"
OUT_H3_MB="${OUT_DIR}/fires_h3.mbtiles"
OUT_POINTS_MB="${OUT_DIR}/fires_points.mbtiles"
MAX_ZOOM="${MAX_ZOOM:-11}"          # tippecanoe max zoom for raw layer
RAW_MIN_ZOOM="${RAW_MIN_ZOOM:-6}"   # tippecanoe min zoom for raw layer (shows at z≥6)
H3_MIN_ZOOM="${H3_MIN_ZOOM:-0}"     # tippecanoe min zoom for H3 layer
H3_RES="${H3_RES:-4}"
LOW_ZOOM_MAX="${LOW_ZOOM_MAX:-7}"
HIGH_ZOOM_MIN="${HIGH_ZOOM_MIN:-$((LOW_ZOOM_MAX + 1))}"

mkdir -p "${OUT_DIR}"
rm -f "${OUT_H3_PM}" "${OUT_H3_PM}-journal" "${OUT_POINTS_PM}" "${OUT_POINTS_PM}-journal"
rm -f "${OUT_H3_MB}" "${OUT_POINTS_MB}"

convert_mbtiles_to_pmtiles() {
  local src="$1"
  local dest="$2"
  if command -v pmtiles >/dev/null 2>&1; then
    echo "Converting ${src} -> ${dest}"
    pmtiles convert "${src}" "${dest}"
    rm -f "${src}"
  else
    echo "ERROR: pmtiles CLI not found. Cannot produce PMTiles archive needed by the viewer."
    echo "Install pmtiles (https://github.com/protomaps/PMTiles) then run: pmtiles convert ${src} ${dest}"
    exit 1
  fi
}

echo "Building H3-only tiles -> ${OUT_H3_PM}"
echo "H3 layer: res=${H3_RES} shown through z${LOW_ZOOM_MAX}"
python "${ROOT_DIR}/scripts/stream_features_h3.py" \
  --h3-res "${H3_RES}" \
  --low-zoom-max "${LOW_ZOOM_MAX}" \
  --high-zoom-min "${HIGH_ZOOM_MIN}" \
  --omit-raw | \
  tippecanoe \
    -o "${OUT_H3_MB}" \
    -Z "${H3_MIN_ZOOM}" \
    -z "${LOW_ZOOM_MAX}" \
    --drop-densest-as-needed \
    --extend-zooms-if-still-dropping \
    -pk -pS -r1 \
    --force \
    --name="Fire events H3" \
    --layer=fires_h3 \
    /dev/stdin
convert_mbtiles_to_pmtiles "${OUT_H3_MB}" "${OUT_H3_PM}"

echo "Building raw-point tiles -> ${OUT_POINTS_PM}"
python "${ROOT_DIR}/scripts/stream_features.py" \
  | tippecanoe \
    -o "${OUT_POINTS_MB}" \
    -Z "${RAW_MIN_ZOOM}" \
    -z "${MAX_ZOOM}" \
    --drop-densest-as-needed \
    --extend-zooms-if-still-dropping \
    -pk -pS -r1 \
    --force \
    --name="Fire events points" \
    --layer=fires_points \
    /dev/stdin
convert_mbtiles_to_pmtiles "${OUT_POINTS_MB}" "${OUT_POINTS_PM}"

echo "Validating H3 coverage vs raw features..."
python "${ROOT_DIR}/scripts/validate_h3_coverage.py"

echo "Done."
