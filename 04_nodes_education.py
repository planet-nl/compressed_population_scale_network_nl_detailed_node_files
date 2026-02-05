"""
Authors: Eszter Bokanyi, e.bokanyi@liacs.leidenuniv.nl, Yuliia Kazmina, y.kazmina@uva.nl
Last modified: 2025.11.20

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
from time import time
import sys
sys.stdout.reconfigure(encoding="utf-8")
import os
import pyreadstat

year = int(sys.argv[1])
output_folder = sys.argv[2]

# Load education code conversion tables (for years 2009-2012)
# These tables map old OPLNRHB codes to standardized OPLNIVSOI2016AGG4HB codes
# Path is within CBS Microdata secure environment
path = 'K:\\Utilities\\Code_Listings\\SSBreferentiebestanden\\'

# First conversion: OPLNR to CTO (Centrale Toelatingsclassificatie Onderwijs)
conversion_df_1, meta_1 = pyreadstat.read_sav(os.path.join(path,'OPLEIDINGSNRREFV34.SAV'),apply_value_formats=False,usecols = ['OPLNR','CTO2016V'])
conversion_df_1 = conversion_df_1.drop(0)  # Drop header row
conversion_df_1.columns = ['OPLNRHB_str','CTO']

# Second conversion: CTO to standardized education level
conversion_df_2, meta_2 = pyreadstat.read_sav(os.path.join(path,'CTOREFV13.sav'),usecols = ['CTO','OPLNIVSOI2016AGG4HB'])

# Merge conversion tables to create full mapping chain
conversion_df = conversion_df_1.merge(conversion_df_2, on='CTO',how='left')
conversion_pl = pl.from_pandas(conversion_df)

educ_files = {2009: r"G:\Onderwijs\HOOGSTEOPLTAB\2009\120619 HOOGSTEOPLTAB 2009V1.csv",
              2010: r"G:\Onderwijs\HOOGSTEOPLTAB\2010\120918 HOOGSTEOPLTAB 2010V1.csv",
              2011: r"G:\Onderwijs\HOOGSTEOPLTAB\2011\130924 HOOGSTEOPLTAB 2011V1.csv",
              2012: r"G:\Onderwijs\HOOGSTEOPLTAB\2012\141020 HOOGSTEOPLTAB 2012V1.csv", 
              2013: r"G:\Onderwijs\HOOGSTEOPLTAB\2013\HOOGSTEOPL2013TABV3.csv", 
              2014: r"G:\Onderwijs\HOOGSTEOPLTAB\2014\HOOGSTEOPL2014TABV3.csv",
              2015: r"G:\Onderwijs\HOOGSTEOPLTAB\2015\HOOGSTEOPL2015TABV3.csv",
              2016: r"G:\Onderwijs\HOOGSTEOPLTAB\2016\HOOGSTEOPL2016TABV2.csv",
              2017: r"G:\Onderwijs\HOOGSTEOPLTAB\2017\HOOGSTEOPL2017TABV3.csv", 
              2018: r"G:\Onderwijs\HOOGSTEOPLTAB\2018\HOOGSTEOPL2018TABV3.csv", 
              2019: r"G:\Onderwijs\HOOGSTEOPLTAB\2019\HOOGSTEOPL2019TABV2.csv",
              2020: r"G:\Onderwijs\HOOGSTEOPLTAB\2020\HOOGSTEOPL2020TABV2.csv",  
              2021: r"G:\Onderwijs\HOOGSTEOPLTAB\2021\HOOGSTEOPL2021TABV2.csv",  
              2022: r"G:\Onderwijs\HOOGSTEOPLTAB\2022\HOOGSTEOPL2022TABV2.csv", 
              2023: r"G:\Onderwijs\HOOGSTEOPLTAB\2023\HOOGSTEOPL2023TABV2.csv", 
              2024: r"G:\Onderwijs\HOOGSTEOPLTAB\2024\HOOGSTEOPL2024TABV1.csv"}

educ_column =  {2009: "OPLNRHB",
                2010: "OPLNRHB",
                2011: "OPLNRHB",
                2012: "OPLNRHB",
                2013:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2014:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2015:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2016:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2017:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2018:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2019:"OPLNIVSOI2021AGG4HBmetNIRWO",
                2020:"OPLNIVSOI2021AGG4HBmetNIRWO",
                2021:"OPLNIVSOI2021AGG4HBmetNIRWO",
                2022:"OPLNIVSOI2021AGG4HBmetNIRWO",
                2023:"OPLNIVSOI2021AGG4HBmetNIRWO",
                2024:"OPLNIVSOI2021AGG4HBmetNIRWO"}

conv_column =  {2009: "OPLNIVSOI2016AGG4HB",
                2010: "OPLNIVSOI2016AGG4HB",
                2011: "OPLNIVSOI2016AGG4HB",
                2012: "OPLNIVSOI2016AGG4HB",
                2013:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2014:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2015:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2016:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2017:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2018:"OPLNIVSOI2016AGG4HBMETNIRWO",
                2019:"OPLNIVSOI2021AGG4HBmetNIRWO",
                2020:"OPLNIVSOI2021AGG4HBmetNIRWO",
                2021:"OPLNIVSOI2021AGG4HBmetNIRWO",
                2022:"OPLNIVSOI2021AGG4HBmetNIRWO",
                2023:"OPLNIVSOI2021AGG4HBmetNIRWO",
                2024:"OPLNIVSOI2021AGG4HBmetNIRWO"}

# Load education data for the given year
# Note: 2023 uses semicolon separator, other years use default
if year == 2023:
    education_input = pl.read_csv(educ_files[year], columns = ['RINPERSOON',educ_column[year],'GEWICHTHOOGSTEOPL'],separator=";")
else:
    education_input = pl.read_csv(educ_files[year], columns = ['RINPERSOON',educ_column[year],'GEWICHTHOOGSTEOPL'])
print(f"Loaded data for {year}")

# Process education codes based on year
# Years 2013+ already have standardized codes, just extract first character
if year > 2012:
    education_input = (
        education_input.
        with_columns(
            pl.col(educ_column[year]).cast(pl.Utf8).str.slice(0,1).alias("educ_level")
        )
        .rename({
            "GEWICHTHOOGSTEOPL":"educ_weight",
            "RINPERSOON":"label"}
        )
        .drop(educ_column[year])
        .with_columns(pl.col("label").cast(pl.Int64))
    )
    
else:
    # Years 2009-2012: require conversion from OPLNRHB to standardized codes
    # Zero-pad OPLNRHB codes to 5 digits for consistent joining
    education_input = (education_input
        .with_columns(
            pl.col("OPLNRHB").cast(pl.Utf8).str.zfill(5).alias("OPLNRHB_str")
        )
    )

    # Join with conversion tables to get standardized education level
    education_input = education_input.join(conversion_pl,on="OPLNRHB_str",how="inner")

    education_input = (
        education_input.with_columns(
            pl.col(conv_column[year]).cast(pl.Utf8).str.slice(0,1).alias("educ_level")
        )
        .drop(['OPLNRHB','OPLNRHB_str','CTO','OPLNIVSOI2016AGG4HB'])
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