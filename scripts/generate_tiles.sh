#!/bin/bash
gdalbuildvrt output/buildings.vrt output/buildings_merged/*.tif
gdal2tiles.py --profile=mercator --processes=16 --resampling=near --resume --zoom=0-17 --srcnodata=0 --s_srs="EPSG:3857" output/buildings.vrt output/buildings_tiles