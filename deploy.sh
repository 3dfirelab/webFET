#!/bin/bash
#
source ~/miniconda3/bin/activate fci
cd /home/paugam/WebSite/leaflet_MED2/

python scripts/generate_manifest.py
scripts/build_tiles_monthly.sh

python3 -m http.server 8008
