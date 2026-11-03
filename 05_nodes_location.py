"""
Author: Eszter Bokanyi, e.bokanyi@liacs.leidenuniv.nl
Last modified: 2026.11.03

This script gets the location code, thus buurt, wijk, and gemeente
of the address of nodes on the reference date of a given year (same reference date as used for
the population in script 02; settings.address_reference in config.json).

Note, that buurt, wijk, and gemeente codes are selected for that
given year, since delineations and coding might change from year
to year. Thus, when joining with geographical data, the correct
year's administrative delineations have to be chosen.

Input:
------
    * address database: latest GBAADRESOBJECTBUS csv (see config.json)
    * address to buurt database: VSLGWBTAB (see config.json)

Output:
-------
    * dataframe saved in {working_folder}\\temp\\location_{year}.csv.gz
        * label (RINPERSOON)
        * buurt_code
        * wijk_code
        * gemeente_code
        * household_change_year

Usage:
------
    /c/mambaforge/envs/9629/python.exe 05_nodes_location.py 2009 /h/ODISSEI_portal_C
        * first argument is year, second is working_folder

Bash script:
------------

for year in `seq 2009 2023`
do
    /c/mambaforge/envs/9629/python.exe 05_nodes_location.py $year /h/ODISSEI_portal_C
done
"""

import pandas as pd
import polars as pl
import sys
sys.stdout.reconfigure(encoding="utf-8")
from pipeline_config import load_config, dataset_path, read_addresses_on_reference_date
import os

year = int(sys.argv[1])
output_folder = sys.argv[2]

# CHG-2026-11: file paths and reference date come from config.json
cfg = load_config(output_folder)

print(f"YEAR {year}")

# CHG-2026-11: addresses valid on the reference date, from the latest GBAADRESOBJECTBUS csv,
# through the same function that script 02 uses
nodes_address = read_addresses_on_reference_date(cfg, year)
print(nodes_address.head())

# Load address-to-buurt mapping file
# VSLGWBTAB contains mappings from address object numbers to geographic codes
# Path is within CBS Microdata secure environment (G: drive)
print("Reading address lookup file and creating dict...")
address_to_buurt = pd.read_spss(
    dataset_path(cfg, "vslgwbtab"),
    usecols = ["SOORTOBJECTNUMMER","RINOBJECTNUMMER",f"bc{year}"],
    convert_categoricals=False)
address_to_buurt = pl.DataFrame({c:address_to_buurt[c].values for c in ["SOORTOBJECTNUMMER","RINOBJECTNUMMER",f"bc{year}"]})
print("Done.")

# deriving clean gemeente, buurt and wijk codes from joining the above two tables
# cleaning columns
# dropping unnecessary columns
print("Getting buurtcodes for selected addresses...")
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
