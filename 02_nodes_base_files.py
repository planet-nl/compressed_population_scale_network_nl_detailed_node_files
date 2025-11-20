"""
Author: Eszter Bokanyi, e.bokanyi@uva.nl, 2025.05.26.

This file creates node dataframes using GBAPERSOONTAB, GBAADRESOBJECTBUS, KINDOUDERTAB, and GBAOVERLIJDENTAB
for each year of the person network edgelists.

Prerequisites: "{working_folder}/temp/merged_node_mapping_{start_year}_{end_year}.csv.gz", which contains the 
mapping of the union of all RINPERSOON labels from the GBAPERSOONTAB {start_year}-{end_year} files to integer ids.

For each year, it selects people active in the GBA on 01-01-JJJJ based on whether they were
registered at any address on this date and it checks. It creates a flag stored in the "active" column of the 
resulting node dataframe that is True is the person is present in year JJJJ.

Along with this flag, it saves the following columns:
* gender
* number of parents from abroad
* birth year
* migrant generation
* missing_mother
* missing_father

Input:
------
    * merged_node_mapping_{start_year}_{end_year}.csv.gz (output of 00_nodes_merged_nodelist.py script)
    * GBAPERSOONTAB selected year
    * GBAADRESOBJECTBUS selected year - 1
    * GBAOVERLIJDENBUS selected year - 1
    * KINDOUDERTAB

Output:
-------
    * {output_folder}\\temp\\base_start_{start_year}_end_{end_year}_year_{year}.csv.gz
        * label
        * active (if True, person is in the nodelist of the given year)
        * gender
        * birth year
        * migrant generation
        * number of parents from abroad
        * whether mother has ever been recorded in the GBA
        * whether father has ever been recorded in the GBA

Usage:
------
    /c/mambaforge/envs/9629/python.exe 02_nodes_base_files.py 2009 2023 2023 /h/ODISSEI_portal_C

Arguments:
----------
    start_year
    end_year
    actual_year
    input_folder
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

# all RINPERSOON labels encoded to integer IDs 2009-2021
merged_nodes = pl.read_csv(
    f"{input_folder}/temp/merged_node_mapping_{start_year}_{end_year}.csv.gz",
    has_header=True
)
print("MERGED NODE FILE HEAD")
print(merged_nodes.head())

# some years do not follow the file naming convention
persoontab_files = {
    2008 : "G:\\Bevolking\\GBAPERSOONTAB\\2009\\GBAPERSOON2009TABV1.sav",
    2016 : "G:\\Bevolking\\GBAPERSOONTAB\\2016\\GBAPERSOONTAB2016V1.sav",
    2018 : "G:\\Bevolking\\GBAPERSOONTAB\\2018\\GBAPERSOON2018TABV2.sav",
    2020 : "G:\\Bevolking\\GBAPERSOONTAB\\2020\\GBAPERSOON2020TABV3.sav",
    2022 : "G:\\Bevolking\\GBAPERSOONTAB\\2022\\GBAPERSOON2022TABV2.sav",
}

overlijdentab_files = {
    2008: "G:\\Bevolking\\GBAOVERLIJDENTAB\\2022\\GBAOVERLIJDEN2022TABV1.sav",
    2009: "G:\\Bevolking\\GBAOVERLIJDENTAB\\2022\\GBAOVERLIJDEN2022TABV1.sav",
    2010: "G:\\Bevolking\\GBAOVERLIJDENTAB\\2022\\GBAOVERLIJDEN2022TABV1.sav",
    2011: "G:\\Bevolking\\GBAOVERLIJDENTAB\\2022\\GBAOVERLIJDEN2022TABV1.sav",
    2012: "G:\\Bevolking\\GBAOVERLIJDENTAB\\2022\\GBAOVERLIJDEN2022TABV1.sav",
    2013: "G:\\Bevolking\\GBAOVERLIJDENTAB\\2022\\GBAOVERLIJDEN2022TABV1.sav",
    2014: "G:\\Bevolking\\GBAOVERLIJDENTAB\\2022\\GBAOVERLIJDEN2022TABV1.sav",
    2015: "G:\\Bevolking\\GBAOVERLIJDENTAB\\2022\\GBAOVERLIJDEN2022TABV1.sav",
    2016: "G:\\Bevolking\\GBAOVERLIJDENTAB\\2022\\GBAOVERLIJDEN2022TABV1.sav",
    2017: "G:\\Bevolking\\GBAOVERLIJDENTAB\\2022\\GBAOVERLIJDEN2022TABV1.sav",
    2018: "G:\\Bevolking\\GBAOVERLIJDENTAB\\2022\\GBAOVERLIJDEN2022TABV1.sav",
    2019: "G:\\Bevolking\\GBAOVERLIJDENTAB\\2022\\GBAOVERLIJDEN2022TABV1.sav",
    2020: "G:\\Bevolking\\GBAOVERLIJDENTAB\\2022\\GBAOVERLIJDEN2022TABV1.sav",
    2021: "G:\\Bevolking\\GBAOVERLIJDENTAB\\2022\\GBAOVERLIJDEN2022TABV1.sav", 
    2022: "G:\\Bevolking\\GBAOVERLIJDENTAB\\2022\\GBAOVERLIJDEN2022TABV1.sav",
    2023: "G:\\Bevolking\\GBAOVERLIJDENTAB\\2023\\GBAOVERLIJDEN2023TABV1.sav",
    2024: "G:\\Bevolking\\GBAOVERLIJDENTAB\\2024\\GBAOVERLIJDEN2024TABV1.sav"
}

objectbus_files = {
    2008 : "G:\\Bevolking\\GBAADRESOBJECTBUS\\GBAADRESOBJECT2009BUSV1.sav"
}

print("========================================")
print(f"YEAR: {year}")
print("========================================")
# columns from the GBAPERSOONTAB
print("Reading GBAPERSOONTAB...")
gbapersoontab_cols = ["RINPERSOON", "GBAGENERATIE", "GBAGESLACHT", "GBAAANTALOUDERSBUITENLAND","GBAGEBOORTEJAAR"]
if year not in persoontab_files.keys():
    fn =  f'G:\Bevolking\GBAPERSOONTAB\{year}\GBAPERSOON{year}TABV1.sav' # terugzetten naar TABV1
else:
    fn  = persoontab_files[year]

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

# rename columns to human readable
nodes.columns = ["label", "gender", "number_of_parents_from_abroad","migrant_generation", "birth_year"]
print("\tFILE HEAD")
nodes = nodes.with_columns(pl.col("label").cast(pl.Int32))
print(nodes.head())
print("\tDone.")

# who was registered at an address at JJJJ-01-01
print("\tReading GBAADRESBUS...")
if year-1 not in objectbus_files:
    fn = f'G:\Bevolking\GBAADRESOBJECTBUS\GBAADRESOBJECT{year-1}BUSV1.sav'
else:
    fn = objectbus_files[year-1]

nodes_address = pl.DataFrame(
    pd.read_spss(
        fn,
        convert_categoricals=False # if this is True, then values such as gender are set to their string values
    )
)
print("\tDone.")
print("\tFiltering population on previous year's dec 31 from ADRESBUS...")
population_jan1_tentative = set(
    nodes_address\
        .filter(
            (pl.col('GBADATUMEINDEADRESHOUDING') >= f"{year-1}1231") & \
            (pl.col("GBADATUMAANVANGADRESHOUDING") <= f"{year-1}1231"))\
        .with_columns(
            pl.col("RINPERSOON").cast(pl.Int32)
        )\
        .get_column("RINPERSOON")\
        .to_list()
)
del nodes_address
print("\tDone.")

# reading latest death tab (2024)
# it should contain the data form the earlier years
last_year = max(list(overlijdentab_files.keys()))
fn = overlijdentab_files[last_year]

print(f"Reading OVERLIJDENTAB year {last_year}, from file name {fn}...")
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
        .filter(pl.col('GBADatumOverlijden') <= f"{year}0101")\
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
kindoudertab = pl.DataFrame(
        pd.read_spss("G:\\Bevolking\\KINDOUDERTAB\\KINDOUDER2024TABV1.sav",convert_categoricals=False)  
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
        pl.col("missing_mother").fill_null(0).cast(pl.Int8),
        pl.col("missing_father").fill_null(0).cast(pl.Int8),
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
# # Sanity checks
# print("First: is active node, Second: has birth year")
# print("\t\t++",sum(nodes["active"]&nodes["birth_year"]))
# print("\t\t+-",sum(nodes["active"]&pd.isnull(nodes["birth_year"])))
# print("\t\t--",sum(~nodes["active"]&pd.isnull(nodes["birth_year"])))
# print("\t\t-+",sum(~nodes["active"]&nodes["birth_year"]))
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
