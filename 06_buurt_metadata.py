"""
Author: Eszter Bokanyi, e.bokanyi@liacs.leidenuniv.nl
Last modified: 2026.11.03

This script gets metadata for the buurten for all years.
It can be later joined to the location data.

Input:
------
    * GIS shapefiles from "K:\\Utilities\\Tools\\GISHulpbestanden\\Gemeentewijkbuurt\\{year}\\"

Output:
-------
    * buurt dataframe saved in {working_folder}\\temp\\buurt_metadata_{year}.csv.gz

Usage:
------
    /c/mambaforge/envs/9629/python.exe 06_buurt_metadata.py 2009 /h/ODISSEI_portal_C
        * first argument is year, second is working_folder

Bash script:
------------

for year in `seq 2009 2023`
do
    /c/mambaforge/envs/9629/python.exe 06_buurt_metadata.py $year /h/ODISSEI_portal_C
done

"""


import pandas as pd
import geopandas as gpd
import numpy as np
import sys
sys.stdout.reconfigure(encoding="utf-8")
from pipeline_config import load_config, dataset_path


year = int(sys.argv[1])
output_folder = sys.argv[2]

print(f"==================== YEAR {year} ===============================")

# CHG-2026-11: shapefile path of the year (naming conventions vary, see config.json)
cfg = load_config(output_folder)
fn = dataset_path(cfg, "gis_buurt", year)

# reading in geopandas file
gdf = gpd.read_file(fn)

# look into file
print(gdf.head())

# renaming
gdf.rename(
    columns = dict(
            STATCODE="buurt_code",
            BU_NAAM="buurt_name",
            BU_CODE="buurt_code"
        ),
    inplace=True
)

# repairing geometries, dropping strange entries
gdf = gdf\
    .set_index("buurt_name")\
    .drop(["Buitenland"],errors="ignore")\
    .reset_index()\
    .dissolve(by="buurt_code")\
    .dropna(subset="buurt_name")\
    .reset_index()\
    .sort_values(by="buurt_code")\
    .reset_index(drop=True)
gdf["geometry"] = gdf["geometry"].buffer(0)

# calculating Amersfoort and lon/lat centroid coordinates
gdf["centroid"] = gdf["geometry"].centroid
gdf["buurt_centroid_x"] = gdf["centroid"].map(lambda p: p.x)
gdf["buurt_centroid_y"] = gdf["centroid"].map(lambda p: p.y)
gdf.set_geometry("centroid",inplace=True)
gdf.set_crs("epsg:28992",inplace=True)
gdf.to_crs("epsg:4326",inplace=True)
gdf["buurt_centroid_lon"] = gdf["centroid"].map(lambda p: p.x)
gdf["buurt_centroid_lat"] = gdf["centroid"].map(lambda p: p.y)

# effective radius: sqrt(area/pi) - error metric for buurt centroid coord in meters
gdf["buurt_eff_r"] = np.sqrt(gdf["geometry"].area / np.pi)

# variables to save in simple dataframe
var_of_interest = ["buurt_code","buurt_name","buurt_centroid_x","buurt_centroid_y","buurt_centroid_lat","buurt_centroid_lon","buurt_eff_r"]

# saving results
pd.DataFrame(gdf[var_of_interest]).to_csv(f"{output_folder}\\temp\\buurt_metadata_{year}.csv.gz",index=False,header=True,compression="gzip")