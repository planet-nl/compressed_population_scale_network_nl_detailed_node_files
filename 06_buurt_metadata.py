"""
Author: Eszter Bokanyi, e.bokanyi@liacs.leidenuniv.nl
Last modified: 2025.11.20

This script gets metadata for the buurten for all years.
It can be later joined to the location data.

Input:
------
    * 

Output:
-------
    * buurt dataframe
            "H:\\shared_data\\nodelists\\location_metadata_{year}.csv.gz", and
            

Usage:
------
    /c/mambaforge/envs/9629/python.exe /h/ebyi/04_buurt_metadata.py 2009
        * first argument is year

Bash script:
------------

for year in `seq 2022 2023`
do
    /c/mambaforge/envs/9629/python.exe /h/ebyi/04_buurt_metadata.py $year
done

"""


import pandas as pd
import geopandas as gpd
import numpy as np
import sys
sys.stdout.reconfigure(encoding="utf-8")


year = int(sys.argv[1])
output_folder = sys.argv[2]

print(f"==================== YEAR {year} ===============================")

# Shapefile names for each year (naming conventions vary)
# Files are in CBS Microdata utilities folder (K: drive)
fn_dict = {
    2009 : "bu_2009.shp",
    2010 : "bu_2010.shp",
    2011 : "bu_2011.shp",
    2012 : "bu_2012.shp",
    2013 : "buurt_2013.shp",
    2014 : "buurt_2014.shp",
    2015 : "buurt_2015.shp",
    2016 : "buurt_2016.shp",
    2017 : "buurt_2017.shp",
    2018 : "buurt2018.shp",
    2019 : "buurt_2019_v1.shp",
    2020 : "bu_2020.shp",
    2021 : "bu_2021.shp",
    2022 : "bu_2022.shp",
    2023 : "bu_2023.shp",
    2024 : "bu_2024.shp"
}

# Construct full path to shapefile
# Path is within CBS Microdata utilities folder (K: drive)
fn = f"K:\\Utilities\\Tools\\GISHulpbestanden\\Gemeentewijkbuurt\{year}\{fn_dict[year]}"

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