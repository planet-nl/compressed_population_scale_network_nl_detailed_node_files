"""
Author: Eszter Bokanyi, e.bokanyi@liacs.leidenuniv.nl
Last modified: 2026.11.03

This script gets metadata for the gemeenten for all years.
It can be later joined to the location data.

Input:
------
    * GIN utility files from "K:\\Utilities\\HULPbestanden\\GebiedeninNederland\\"

Output:
-------
    * gemeente dataframe and codebook as json saved in 
            {working_folder}\\temp\\gemeente_metadata_{year}.csv.gz, and
            {working_folder}\\codebook\\gemeente_metadata_codebook_{year}.json
        * "gemeente"
        * "landsdeel"
        * "provincie"
        * "coropgebied"
        * "nuts1"
        * "nuts2"
        * "nuts3"
        * "stedgem"

Usage:
------
    /c/mambaforge/envs/9629/python.exe 07_gemeente_metadata.py 2022 /h/ODISSEI_portal_C
        * first argument is year, second is working_folder

Bash script:
------------

for year in `seq 2009 2023`
do
    /c/mambaforge/envs/9629/python.exe 07_gemeente_metadata.py $year /h/ODISSEI_portal_C
done

"""


import pandas as pd
import sys
sys.stdout.reconfigure(encoding="utf-8")
from pipeline_config import load_config, dataset_path
import json
import numpy as np

year = int(sys.argv[1])
output_folder = sys.argv[2]

# GIN (Gebieden in Nederland) file path of the year comes from config.json # CHG-2026-11
# Files are in CBS Microdata utilities folder
# File format changes over time: SAV (2009-2018), DTA (2019-2020), XLSX (2021+)
cfg = load_config(output_folder)

print(f"==================== YEAR {year} ===============================")

# filename
fn = dataset_path(cfg, "gin", year)

# selected variables
var_of_interest = ["gemeente_code","landsdeel","provincie","coropgebied","stedgem"]
if year == 2019:
    var_of_interest_input= ["gemeentencode","landsdelencode","provinciescode","coropgebiedencode","stedelijkheidcode"]
    name_of_interest = ["gemeentenenaam","landsdelennaam","provinciesnaam","coropgebiedennaam","stedelijkheidomschrijving"]
if year == 2020:
    var_of_interest_input= ["gemeentencode","landsdelencode","provinciescode","coropgebiedencode","stedelijkheidcode"]
    name_of_interest = ["gemeentennaam","landsdelennaam","provinciesnaam","coropgebiedennaam","stedelijkheidomschrijving"]
if year > 2020:
    var_of_interest_input = ["gemeenten|Code", "Landsdelen|Code", "Provincies|Code", "COROP-gebieden|Code", "Stedelijkheid|Code"]
    name_of_interest = [n.split("|")[0]+"|Naam" for n in var_of_interest_input]
    name_of_interest[-1] = "Stedelijkheid|Omschrijving"
if year == 2021:
    name_of_interest[0] = name_of_interest[0].lower()

# Initialize codebook to store code-to-name mappings
codebook = {}

# Handle SPSS (.sav) files (2009-2018)
# Need to read twice: once for codes, once for labels
if fn.split(".")[-1]=="sav":
    # with categorical variables
    df = pd.read_spss(fn,convert_categoricals=False)

    # with labels
    df_w_labels = pd.read_spss(fn,convert_categoricals=True) 

    if year==2014:
        df.rename(columns={c:c.lower() for c in df.columns},inplace=True)
        df_w_labels.rename(columns={c:c.lower() for c in df_w_labels.columns},inplace=True)
    
    df_w_labels["gemeente_code"] = df_w_labels["gemeente"]

        
    df["gemeente_code"] = "GM" + df["gemeente"].astype(str)
    df["landsdeel"] = df["landsdeel"].astype(int)
    df["provincie"] = df["provincie"].astype(int)
    df["coropgebied"] = df["coropgebied"].astype(int)
    df["stedgem"] = df["stedgem"].astype(int)

    # Create codebook: map codes to human-readable labels
    for c in var_of_interest:
        codebook[c] = {k:v for k,v in set(zip(df[c],df_w_labels[c]))}

# Handle Stata (.dta) files (2019-2020)
elif fn.split(".")[-1]=="dta":
    df = pd.read_stata(fn)
    
    if year < 2019:
        df["gemeente_code"] = "GM" + df["gemeente"].astype(str)
        df["landsdeel"] = df["landsdeel"].astype(int)
        df["provincie"] = df["provincie"].astype(int)
        df["coropgebied"] = df["coropgebied"].astype(int)
        df["stedgem"] = df["stedgem"].astype(int)

    if year>=2019:
        # creating codebook
        for c,cc in zip(var_of_interest_input,name_of_interest):
            codebook[c] = dict(zip(df[c],df[cc]))
    else:
        # creating codebook
        for c in var_of_interest:
            codebook[c] = dict(zip(df[c],df["lab" + c]))

    if year>=2019:
        t = dict(zip(var_of_interest,var_of_interest_input))
        trev = {v:k for k,v in t.items()}
        print(json.dumps(trev,indent=4))
        df.rename(columns=trev,inplace=True)
        print(df.head())
        for c in ["landsdeel","provincie","coropgebied"]:
            df[c] = df[c].str.slice(2,4).map(int)
    
        df["gemeente_code"] = "GM" + df["gemeente"].astype(str)
        df["landsdeel"] = df["landsdeel"].astype(int)
        df["provincie"] = df["provincie"].astype(int)
        df["coropgebied"] = df["coropgebied"].astype(int)
        df["stedgem"] = df["stedgem"].astype(int)
        
        for c in var_of_interest:
            codebook[c] = codebook[t[c]]
            del codebook[t[c]]

# Handle CSV files (not currently used but included for completeness)
elif fn.split(".")[-1]=="csv":
    df = pd.read_csv(fn,header=0,index_col=None)
    for c in ["landsdeel","provincie","coropgebied"]:
        df[c] = df[c].str.slice(2,4).map(int)

    df["gemeente_code"] = "GM" + df["gemeente"].astype(str)
    df["landsdeel"] = df["landsdeel"].astype(int)
    df["provincie"] = df["provincie"].astype(int)
    df["coropgebied"] = df["coropgebied"].astype(int)
    df["stedgem"] = df["stedgem"].astype(int)

    # Create codebook from CSV columns
    for c in var_of_interest:
        codebook[c] = dict(zip(df[c],df[c+"naam"]))

# Handle Excel (.xlsx) files (2021+)
elif fn.split(".")[-1]=="xlsx":
    df = pd.read_excel(fn)
    df = df.rename(columns = {c : c.strip(" ") for c in df.columns})
    print(df.head())
    print(df.columns)
    df = df.rename(columns=dict(zip(var_of_interest_input,var_of_interest)))
    mask = ~pd.isnull(df["gemeente_code"]) & ~np.isnan(df["gemeente_code"])
    df = df[mask].copy()


    for c in ["landsdeel","provincie","coropgebied"]:
        df[c] = df[c].str.slice(2,4)

    df["gemeente_code"] = "GM" + df["gemeente_code"].astype(int).astype(str).str.zfill(4)
    df["landsdeel"] = df["landsdeel"].astype(int).astype(str)
    df["stedgem"] = df["stedgem"].astype(int).astype(str).str.zfill(1)
    
    # creating codebook
    for c,cc in zip(var_of_interest,name_of_interest):
        codebook[c] = dict(zip(df[c],df[cc]))

print(df[var_of_interest].head())
print(df[var_of_interest].dtypes)

print("NUMBER of GEMEENTE: ",len(pd.unique(df["gemeente_code"])))
# print(json.dumps(codebook,indent=4))

# saving results
df[var_of_interest].to_csv(f"{output_folder}\\temp\\gemeente_metadata_{year}.csv.gz",index=False,header=True,compression="gzip")
json.dump(codebook,open(f"{output_folder}\\codebook\\gemeente_metadata_codebook_{year}.json","w"),indent=4)