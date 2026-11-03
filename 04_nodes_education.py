"""
Authors: Eszter Bokanyi, e.bokanyi@liacs.leidenuniv.nl, Yuliia Kazmina, y.kazmina@uva.nl
Last modified: 2026.11.03

This script extracts highest education level data and converts education codes
to a standardized 4-level classification system across different years.

Education coding systems changed over time:
- 2009-2012: Uses OPLNRHB codes requiring conversion via reference tables
- 2013-2018: Uses OPLNIVSOI2016AGG4HBMETNIRWO
- 2019+: Uses OPLNIVSOI2021AGG4HBmetNIRWO

Input:
------
    * HOOGSTEOPLTAB files for the given year
    * Education conversion tables:
        - OPLEIDINGSNRREFV34.SAV (education number reference)
        - CTOREFV13.sav (CTO reference)

Output:
-------
    * {output_folder}/temp/education_{year}.csv.gz
        * label (RINPERSOON)
        * educ_level (single character: education level code)
        * educ_weight (weight for education record)

Arguments:
----------
    year: Year to process
    output_folder: Base directory for outputs

Usage:
------
    /c/mambaforge/envs/9629/python.exe 04_nodes_education.py 2015 /h/ODISSEI_portal_C

Bash script:
------------

for year in `seq 2009 2023`
do
    /c/mambaforge/envs/9629/python.exe 04_nodes_education.py $year /h/ODISSEI_portal_C
done
"""

import pandas as pd
import polars as pl
import sys
sys.stdout.reconfigure(encoding="utf-8")
from pipeline_config import load_config, dataset_path, dataset_option
import os
import pyreadstat

year = int(sys.argv[1])
output_folder = sys.argv[2]

# CHG-2026-11: file paths, column names per period and reference tables come from config.json
cfg = load_config(output_folder)
edu_cfg = cfg["education"]
period = [p for p in edu_cfg["periods"] if p["from"] <= year <= p["to"]]
assert len(period) == 1, f"No (unique) education coding period for year {year} in config.json"
period = period[0]

# Load education code conversion tables (only for years that need it, 2009-2012)
# These tables map old OPLNRHB codes to standardized OPLNIVSOI2016AGG4HB codes
# Path is within CBS Microdata secure environment
if period["needs_conversion"]:
    path = edu_cfg["conversion_dir"]

    # First conversion: OPLNR to CTO (Centrale Toelatingsclassificatie Onderwijs)
    conversion_df_1, meta_1 = pyreadstat.read_sav(os.path.join(path,edu_cfg["oplnr_to_cto_file"]),apply_value_formats=False,usecols = ['OPLNR','CTO2016V'])
    conversion_df_1 = conversion_df_1.drop(0)  # Drop header row
    conversion_df_1.columns = ['OPLNRHB_str','CTO']

    # Second conversion: CTO to standardized education level
    conversion_df_2, meta_2 = pyreadstat.read_sav(os.path.join(path,edu_cfg["cto_to_level_file"]),usecols = ['CTO',period["converted_column"]])

    # Merge conversion tables to create full mapping chain
    conversion_df = conversion_df_1.merge(conversion_df_2, on='CTO',how='left')
    conversion_pl = pl.from_pandas(conversion_df)

# Load education data for the given year
# Note: separator (e.g. ';' for 2023) is year-specific, see config.json
education_input = pl.read_csv(
    dataset_path(cfg, "hoogsteopltab", year),
    columns = ['RINPERSOON',period["column"],'GEWICHTHOOGSTEOPL'],
    separator = dataset_option(cfg, "hoogsteopltab", "sep", year, default=",")
)
print(f"Loaded data for {year}")

# Process education codes based on year
# Years 2013+ already have standardized codes, just extract first character
if not period["needs_conversion"]:
    education_input = (
        education_input.
        with_columns(
            pl.col(period["column"]).cast(pl.Utf8).str.slice(0,1).alias("educ_level")
        )
        .rename({
            "GEWICHTHOOGSTEOPL":"educ_weight",
            "RINPERSOON":"label"}
        )
        .drop(period["column"])
        .with_columns(pl.col("label").cast(pl.Int64))
    )
    
else:
    # Years 2009-2012: require conversion from OPLNRHB to standardized codes
    # Zero-pad OPLNRHB codes to 5 digits for consistent joining
    education_input = (education_input
        .with_columns(
            pl.col(period["column"]).cast(pl.Utf8).str.zfill(5).alias("OPLNRHB_str")
        )
    )

    # Join with conversion tables to get standardized education level
    education_input = education_input.join(conversion_pl,on="OPLNRHB_str",how="inner")

    education_input = (
        education_input.with_columns(
            pl.col(period["converted_column"]).cast(pl.Utf8).str.slice(0,1).alias("educ_level")
        )
        .drop([period["column"],'OPLNRHB_str','CTO',period["converted_column"]])
        .rename({
            "GEWICHTHOOGSTEOPL":"educ_weight",
            "RINPERSOON":"label"}
        ).with_columns(
            pl.col("label").cast(pl.Int64)
        )
    )
    

output = os.path.join(output_folder,"temp",f"education_{year}.csv")
print(f"Saving results to {output}...")
education_input.write_csv(
    output,
    include_header=True
)
print("Done.")

print("\t zipping...")
os.system(f"gzip -f {output}")
print("\tDone.")