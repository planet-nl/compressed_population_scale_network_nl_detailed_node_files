"""
Author: Eszter Bokanyi, e.bokanyi@liacs.leidenuniv.nl
Last modified: 2026.11.03

This file creates node dataframes using GBAPERSOONTAB, GBAADRESOBJECTBUS, KINDOUDERTAB, and GBAOVERLIJDENTAB
for each year of the person network edgelists.

Prerequisites: "{working_folder}/temp/merged_node_mapping_{start_year}_{end_year}.csv.gz", which contains the 
mapping of the union of all RINPERSOON labels from the GBAPERSOONTAB {start_year}-{end_year} files to integer ids.

For each year, it selects people active in the GBA on 01-01-JJJJ based on whether they were
registered at any address on the reference date (see settings.address_reference in config.json). It creates a flag stored in the "active" column of the 
resulting node dataframe that is True if the person is present in year JJJJ.

Along with this flag, it saves the following columns:
* gender
* number of parents from abroad
* birth year
* migrant generation
* missing_mother
* missing_father

Input:
------
    * merged_node_mapping_{start_year}_{end_year}.csv.gz (output of 01_nodes_merged_nodelist.py script)
    * GBAPERSOONTAB selected year
    * GBAADRESOBJECTBUS latest available csv file (cumulative)
    * GBAOVERLIJDENTAB latest available file (cumulative)
    * KINDOUDERTAB latest available file (cumulative)

Output:
-------
    * {output_folder}\\temp\\base_start_{start_year}_end_{end_year}_year_{year}.csv.gz
        * label
        * active (if True, person is in the nodelist of the given year)
        * gender
        * birth year
        * migrant generation
        * number of parents from abroad
        * missing_mother (1 if no mother is linked in KINDOUDERTAB, incl. people not in KINDOUDERTAB)
        * missing_father (1 if no father is linked in KINDOUDERTAB, incl. people not in KINDOUDERTAB)

Usage:
------
    /c/mambaforge/envs/9629/python.exe 02_nodes_base_files.py 2009 2023 2023 /h/ODISSEI_portal_C

Arguments:
----------
    start_year
    end_year
    actual_year
    input_folder (working folder; src/config.json and src/pipeline_config.py are read from here)
    [output_folder]

Bash script:
------------

for year in `seq 2009 2023`
do
    /c/mambaforge/envs/9629/python.exe 02_nodes_base_files.py 2009 2023 $year /h/ODISSEI_portal_C
done
"""

import pandas as pd
import polars as pl
import sys
sys.stdout.reconfigure(encoding="utf-8")
from pipeline_config import load_config, dataset_path, read_addresses_on_reference_date
import os

# getting arguments
start_year = int(sys.argv[1])
end_year = int(sys.argv[2])
year = int(sys.argv[3])
input_folder = sys.argv[4]
if len(sys.argv)==6:
    output_folder = sys.argv[5]
else:
    output_folder = input_folder

# CHG-2026-11: file paths and reference date come from config.json
cfg = load_config(input_folder)

# all RINPERSOON labels encoded to integer IDs for start_year-end_year
merged_nodes = pl.read_csv(
    f"{input_folder}/temp/merged_node_mapping_{start_year}_{end_year}.csv.gz",
    has_header=True
)
print("MERGED NODE FILE HEAD")
print(merged_nodes.head())

print("========================================")
print(f"YEAR: {year}")
print("========================================")
# columns from the GBAPERSOONTAB
print("Reading GBAPERSOONTAB...")
gbapersoontab_cols = ["RINPERSOON", "GBAGENERATIE", "GBAGESLACHT", "GBAAANTALOUDERSBUITENLAND","GBAGEBOORTEJAAR"]
fn = dataset_path(cfg, "gbapersoontab_sav", year)  # CHG-2026-11: was hard-coded dict

print(f"\t... from file {fn}")

try:
    nodes = pl.DataFrame(
        pd.read_spss(
            fn,
            usecols=gbapersoontab_cols,
            convert_categoricals=False # if this is True, then values such as gender are set to their string values
        )
    )
except FileNotFoundError:
    print(f"YEAR {year}: No file {fn} found!")
    raise  # CHG-2026-11

# rename columns to human readable; # CHG-2026-11: by column name, was positional
nodes = nodes.rename({
    "RINPERSOON": "label",
    "GBAGESLACHT": "gender",
    "GBAAANTALOUDERSBUITENLAND": "number_of_parents_from_abroad",
    "GBAGENERATIE": "migrant_generation",
    "GBAGEBOORTEJAAR": "birth_year"
})
print("\tFILE HEAD")
nodes = nodes.with_columns(pl.col("label").cast(pl.Int32))
print(nodes.head())
print("\tDone.")

# who was registered at an address on the reference date (1 Jan of year or 31 Dec of year-1, see config.json; default 1 Jan)
# CHG-2026-11: addresses come from the latest GBAADRESOBJECTBUS csv, through the same function that script 05 uses
print("\tReading GBAADRESOBJECTBUS and filtering population on the reference date...")
population_jan1_tentative = set(
    read_addresses_on_reference_date(cfg, year)
        .with_columns(pl.col("RINPERSOON").cast(pl.Int32))
        .get_column("RINPERSOON")
        .to_list()
)
print("\tDone.")

# reading latest death tab
# it should contain the data form the earlier years
fn = dataset_path(cfg, "gbaoverlijdentab")  # CHG-2026-11: latest cumulative file, from config.json

print(f"Reading OVERLIJDENTAB from file name {fn}...")
overlijdentab = pl.DataFrame(
    pd.read_spss(
        fn,
        convert_categoricals=False
    )
)
print("\tDone.")
print("\tSelecting those who died up until the given year's 1 Jan...")
# dead before 0101
dead = set(
    overlijdentab\
        .filter(pl.col('GBADatumOverlijden').cast(pl.String) <= f"{year}0101")\
        .with_columns(pl.col("RINPERSOON").cast(pl.Int32))\
        .get_column("RINPERSOON").to_list()
)
del overlijdentab
print("\tDone.")

print("\tDeducting dead people from the tentative population...")
population_jan1 = population_jan1_tentative.difference(dead)
print(f"\tRemoved {len(population_jan1_tentative)-len(population_jan1)} people based on the death tab.")
del population_jan1_tentative
del dead
print("\tDone.")

print("\tCreating active column in dataframes...")
# people in the address list but not in the merged node list based on GBAPERSOONTAB files
print("\tPeople in population not in merged_nodes:",len(population_jan1.difference(set(merged_nodes["label"]))))
merged_nodes = merged_nodes.with_columns(
    pl.col("label").is_in(population_jan1).alias("active")
)
# nodes only contains active nodes, we'll merge data, then merge this back to the merged_nodes to contain everyone
nodes = nodes.with_columns(
    pl.col("label").is_in(population_jan1).alias("active")
).filter(pl.col("active"))

print("Reading KINDOUDERTAB...")
# Load parent-child relationship table (most recent version contains all historical data)
# Path is within CBS Microdata secure environment (G: drive)
kindoudertab = pl.DataFrame(
        pd.read_spss(dataset_path(cfg, "kindoudertab"),convert_categoricals=False)  
    )\
    .filter(pl.col("RINPERSOONS")=="R")\
    .with_columns(
        pl.col("RINPERSOON").cast(pl.Int64).alias("label"),
        pl.col("RINPERSOONSMa").map_elements(lambda x: 0 if x=="R" else 1, return_dtype=pl.Int8).alias("missing_mother"),
        pl.col("RINPERSOONSpa").map_elements(lambda x: 0 if x=="R" else 1, return_dtype=pl.Int8).alias("missing_father")
    )\
    .select(
        pl.col("label"),
        pl.col("missing_mother"),
        pl.col("missing_father")
    )
print(kindoudertab.head())
# adding missing parent info to node dataframe
nodes = nodes\
    .join(kindoudertab,on="label",how="left")\
    .with_columns(
        # people not in KINDOUDERTAB have no linked parents, so both parents count as missing
        pl.col("missing_mother").fill_null(1).cast(pl.Int8),
        pl.col("missing_father").fill_null(1).cast(pl.Int8),
        pl.col("birth_year").cast(pl.Int16),
        pl.col("gender").cast(pl.Int8),
        pl.col("number_of_parents_from_abroad").cast(pl.Int8),
        pl.col("migrant_generation").cast(pl.Int8)
    )\
    .select(
        pl.exclude("active")
    )
print("\tDone.")

print("Writing all info back to merged node dataframe...")
nodes = merged_nodes\
    .join(
        nodes,
        on="label",
        how="left"
    )

# Optional sanity check (uncomment to verify data quality):
# Validates that active nodes have demographic data and inactive nodes don't
# print("Sanity check - Active nodes vs birth year presence:")
# print("\t\tActive with birth_year:", sum(nodes["active"] & ~nodes["birth_year"].is_null()))
# print("\t\tActive without birth_year:", sum(nodes["active"] & nodes["birth_year"].is_null()))
# print("\t\tInactive without birth_year:", sum(~nodes["active"] & nodes["birth_year"].is_null()))
# print("\t\tInactive with birth_year:", sum(~nodes["active"] & ~nodes["birth_year"].is_null()))

print("\tDone.")

output = f'{output_folder}\\temp\\base_start_{start_year}_end_{end_year}_year_{year}.csv'
print(f"Writing results to {output}...")
with pl.Config(tbl_cols = -1):
    print(nodes.head())
(nodes
    .select(
        pl.col("label"),
        pl.col("id"),
        pl.col("active"),
        pl.col("gender"),
        pl.col("birth_year"),
        pl.col("migrant_generation"),
        pl.col("number_of_parents_from_abroad"),
        pl.col("missing_mother"),
        pl.col("missing_father")
    )
    .write_csv(output,include_header=True)
)
print("\tDone.")
print("\t zipping...")
os.system(f"gzip -f {output}")
print("\tDone.")
