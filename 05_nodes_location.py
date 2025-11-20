"""
Author: Eszter Bokanyi, e.bokanyi@liacs.leidenuniv.nl
Last modified: 2025.11.20

This script gets the location code, thus buurt, wijk, and gemeente
of the jan 1 address of nodes for a given year.

Note, that buurt, wijk, and gemeente codes are selected for that
given year, since delineations and coding might change from year
to year. Thus, when joining with geographical data, the correct
year's administrative delineations have to be chosen.

Input:
------
    * address database: "G:\Bevolking\GBAADRESOBJECTBUS\GBAADRESOBJECT2023BUSV1.sav"
    * address to buurt database: "G:\BouwenWonen\VSLGWBTAB\VSLGWB2023TAB03V1.sav"

Output:
-------
    * dataframe saved in H:\\shared_data\\nodelists\\location_{year}.csv.gz"
        * label (RINPERSOON)
        * buurt_code
        * wijk_code
        * gemeente_code
        * household_change_year

Usage:
------
    /c/mambaforge/envs/9629/python.exe /h/ebyi/03_nodes_location.py 2009 "H:\\ODISSEI_portal_C"
        * first argument is year

Bash script:
------------

for year in `seq 2022 2023`
do
    /c/mambaforge/envs/9629/python.exe /h/ebyi/03_nodes_location.py $year
done
"""

import pandas as pd
import polars as pl
from time import time
import sys
sys.stdout.reconfigure(encoding="utf-8")
import os

year = int(sys.argv[1])
output_folder = sys.argv[2]

print(f"YEAR {year}")

# Read all registered addresses from the most recent converted CSV file
# Note: Using 2024 file as it contains historical address data for all years
# Path is within CBS Microdata secure environment (G: drive)
fn = f"G:\Bevolking\GBAADRESOBJECTBUS\GBAADRESOBJECT2024BUSV1.csv"
print(f"Reading all addresses from {fn}...")
nodes_address = pl.read_csv(fn,separator=",")
print(f"Done. Total number of records is {nodes_address.shape[0]}.")
print(nodes_address.head())

# addresses on jan 1 of the given year
print("Selecting people's addresses on jan 1...")
tic = time()
nodes_address = nodes_address.filter(
    (pl.col('GBADATUMEINDEADRESHOUDING').cast(pl.String) >= f"{year}0101") &
    (pl.col('GBADATUMAANVANGADRESHOUDING').cast(pl.String) <= f"{year}0101")
)
toc = time()
print(f"Done in {toc-tic:.1f}s.")

# Load address-to-buurt mapping file
# VSLGWBTAB contains mappings from address object numbers to geographic codes
# Path is within CBS Microdata secure environment (G: drive)
print("Reading address lookup file and creating dict...")
address_to_buurt = pd.read_spss(
    f'G:\BouwenWonen\VSLGWBTAB\VSLGWB2023TAB03V1.sav',
    usecols = ["SOORTOBJECTNUMMER","RINOBJECTNUMMER",f"bc{year}"],
    convert_categoricals=False)
address_to_buurt = pl.DataFrame({c:address_to_buurt[c].values for c in ["SOORTOBJECTNUMMER","RINOBJECTNUMMER",f"bc{year}"]})
print("Done.")

# deriving clean gemeente, buurt and wijk codes from joining the above two tables
# cleaning columns
# dropping unnecessary columns
print("Getting buurtcodes for selected jan 1 addresses...")
print("Deriving household change year, buurt code, wijk code, and gemeente code...")
nodes_address = (nodes_address\
    .join(address_to_buurt,on=["SOORTOBJECTNUMMER","RINOBJECTNUMMER"],how="left")\
    .rename({f"bc{year}":"location_code"})\
    .with_columns(
        pl.col("GBADATUMAANVANGADRESHOUDING").cast(pl.String).str.slice(0,4).cast(pl.Int16).alias("household_change_year"),
        pl.col("location_code").map_elements(lambda s: str(s).zfill(8) if s!='NA' else None).alias("location_code")
    )\
    .select(
        pl.exclude(["RINPERSOONS","GBADATUMAANVANGADRESHOUDING","GBADATUMEINDEADRESHOUDING"])
    )\
    .rename({
        "RINPERSOON":"label"
    })\
    .with_columns(
        pl.col("location_code").str.slice(0,4).alias("gemeente_code"),
        pl.col("location_code").str.slice(0,6).alias("wijk_code"),
        pl.col("location_code").str.slice(0,8).alias("buurt_code")
    )\
    .with_columns(
        pl.concat_str([pl.lit("GM"),pl.col("gemeente_code")]).alias("gemeente_code"),
        pl.concat_str([pl.lit("WK"),pl.col("wijk_code")]).alias("wijk_code"),
        pl.concat_str([pl.lit("BU"),pl.col("buurt_code")]).alias("buurt_code")
    )
    .select(
        pl.exclude(["location_code","SOORTOBJECTNUMMER","RINOBJECTNUMMER","GBAFUNCTIEADRES","GBAAANGIFTEADRESHOUDING"])
    )
)
print("Done.")

# saving results
output = f"{output_folder}\\temp\\location_{year}.csv"
print(f"Saving results to {output}...")
with pl.Config(tbl_cols = -1):  
    print(nodes_address.head())
    print(nodes_address.count())
nodes_address.write_csv(
    output,
    include_header=True
)
print("Done.")

print("\t zipping...")
os.system(f"gzip -f {output}")
print("\tDone.")
