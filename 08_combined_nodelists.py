"""
Author: Eszter Bokanyi, e.bokanyi@liacs.leidenuniv.nl
Last modified: 2025.11.20

This script merges all previous node attribute files.

Input:
------
    * year, int

Output:
-------
    * "H:\\shared_data\\nodelists\\combined_{year}.csv.gz
        * has all columns from previous files, plus added location metadata columns

Usage:
------
    /c/mambaforge/envs/9629/python.exe /h/ebyi/05_combined_nodelists.py 2009 2022 2009

Bash script:
------------

for year in `seq 2009 2021`
do
    /c/mambaforge/envs/9629/python.exe /h/ebyi/05_combined_nodelists.py 2009 2022 $year
done

Copying combined nodelists to their final places:
-------------------------------------------------

for year in `seq 2009 2021`
do
    echo "==================== YEAR $year =================================="
    cp /h/shared_data/nodelists/combined_$year.csv.gz /h/shared_data/$year/V2/nodes.csv.gz
    cp /h/shared_data/nodelists/combined_$year.csv.gz /h/shared_data/$year/V2_grouped_undirected/nodes.csv.gz
done
"""

import pandas as pd
import polars as pl
import json
import sys
sys.stdout.reconfigure(encoding="utf-8")
import numpy as np
from time import time
import os

tic = time()


start_year = int(sys.argv[1])
end_year = int(sys.argv[2])
year = int(sys.argv[3])
output_folder = sys.argv[4]


print(f"YEAR {year}", "start year", start_year, "end_year", end_year)

if year<2011:
    has_income=False
else:
    has_income=True

if has_income:
    print(f"Year {year} has income data.")
else:
    print(f"Year {year} does NOT have income data.")

print("Reading node attribute files...")
# base
nodes = pl.read_csv(f"{output_folder}\\temp\\base_start_{start_year}_end_{end_year}_year_{year}.csv.gz",has_header=True)
#income
if has_income:  
    nodes_income = pl.read_csv(f"{output_folder}\\temp\\income_{year}.csv.gz",has_header=True)
    nodes = nodes.join(nodes_income,on="label",how="left")
# education
nodes_education = pl.read_csv(f"{output_folder}\\temp\\education_{year}.csv.gz",has_header=True)
# location
nodes_location = pl.read_csv(f"{output_folder}\\temp\\location_{year}.csv.gz",has_header=True)
buurt_metadata = pl.read_csv(f"{output_folder}\\temp\\buurt_metadata_{year}.csv.gz",has_header=True)
gemeente_metadata = pl.read_csv(f"{output_folder}\\temp\\gemeente_metadata_{year}.csv.gz",has_header=True)

nodes = (nodes
    .join(nodes_education,on="label",how="left")
    .join(nodes_location,on="label",how="left")
    .sort(by="id")
    .with_columns(
        pl.col("number_of_parents_from_abroad").cast(pl.Int32),
        pl.col("missing_mother").cast(pl.Int8),
        pl.col("missing_father").cast(pl.Int8),
        *[pl.col(c).cast(pl.Int64) for c in nodes.columns if "income" in c]
    )
    .join(buurt_metadata.select(pl.exclude("buurt_name")), how="left", on="buurt_code")
    .join(gemeente_metadata, how="left", on="gemeente_code")
)

with pl.Config(tbl_cols=-1):
    print(nodes.head())
print("Done.")

# saving
output = f"{output_folder}\\yearly_node_files\\nodes_start_{start_year}_end_{end_year}_year_{year}.csv"
print(f"Saving merged node attribute dataframe to {output}...")
nodes.write_csv(output)
toc = time()
print(f"Done in {toc-tic:.1f}s.")
tic = toc
print("Zipping...")
os.system(f"gzip -f {output}")
toc = time()
print(f"Done in {toc-tic:.1f}s.")