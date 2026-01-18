#!/bin/bash
#
echo "#############"
echo "run fireEventStats.py --inputName MED2 --sensorName FCI  on ANDALL"
echo "#############"

echo "#############"
echo "no syn data on ESTRELLA"
echo "#############"
rsync -avz --delete andall.cdb.upc.edu:/data/shared/FCI/MED2_fire_events/Stats/GeoJson/ GeoJson-per-event/
rsync -avz --delete andall.cdb.upc.edu:/data/shared/FCI/MED2_fire_events/GeoJson/ GeoJson/
rsync -avz --delete andall.cdb.upc.edu:/data/shared/FCI/MED2_fire_events/FRP-FROS/ FRP-FROS/

rsync -avz --delete andall.cdb.upc.edu:/data/shared/FCI/MED2_fire_events/Stats/ /mnt/data3/FCI/MED2_fire_events/Stats/

#for FETVIEW
rsync -av   --prune-empty-dirs   --include='*/'   --include='web/***'   --include='hotspots/***'   --include='manifest_id_dirname.json'   --exclude='*'   andall.cdb.upc.edu:/data/shared/FCI/MED2_fire_events/FETView/   ./FETView/

echo "#############"
echo "and generate the tile data"
echo "#############"

source ~/miniconda3/bin/activate fci
cd /home/paugam/WebSite/leaflet_MED2/

python scripts/generate_manifest.py
scripts/build_tiles_monthly.sh
