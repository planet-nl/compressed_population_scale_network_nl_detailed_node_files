"""
Author: Eszter Bokanyi, e.bokanyi@liacs.leidenuniv.nl
Last modified: 2025.11.20

This script creates the union of all RINPERSOON ids that occur in the GBA files between 
start_year and end_year given as arguments to the script.

This serves as the basis for node encoding for the longitudinal network files.

Usage example (bash):
---------------------
/c/mambaforge/envs/9629/python.exe 01_nodes_merged_nodelist.py 2009 2023 /h/ODISSEI_portal_C

Arguments:
----------
start_year
end_year
working_folder
[existing_mapping]  optional: csv.gz with columns id and label (ids 0..N-1); saved as the mapping instead of building a new one

Inputs:
-------
GBAPERSOONTAB files each year between start_year and end_year in CSV format
config.json (and pipeline_config.py) in {working_folder}/src: file names, separators and encodings

Output:
-------
{working_folder}/temp/merged_node_mapping_{start_year}_{end_year}.csv.gz

"""

import pandas as pd
import polars as pl
import os
import sys
sys.stdout.reconfigure(encoding="utf-8")
import json
from pipeline_config import load_config, dataset_path, dataset_option

# capturing script arguments
start_year = int(sys.argv[1])
end_year = int(sys.argv[2])
working_folder = sys.argv[3]

# CHG-2026-11: optional 4th argument, an existing id/label mapping (csv.gz) to use instead of building a new one
if len(sys.argv) > 4:
    merged_node_df = pd.read_csv(sys.argv[4], usecols=["id","label"])[["id","label"]].sort_values("id").reset_index(drop=True)    
    # save
    print(f"Saving merged dataframe to " + os.path.join(working_folder,"temp",f"merged_node_mapping_{start_year}_{end_year}.csv.gz")+"...")
    merged_node_df.to_csv(os.path.join(working_folder,"temp",f"merged_node_mapping_{start_year}_{end_year}.csv.gz"),header=True,index=False,compression="gzip")
    print("Done.")           
    sys.exit(0)

# CHG-2026-11: configuration file with file paths, separators and encodings
cfg = load_config(working_folder)

# creating list of years between start_year and end_year
year_list = list(range(start_year,end_year+1))

# set of nodes
node_set = list()

for year in year_list:
    print("YEAR",year)
    # CHG-2026-11: file name (default or year-specific) and separator from config.json
    node_file = dataset_path(cfg, "gbapersoontab_csv", year)
    print(f"Reading {node_file}...")
    sep = dataset_option(cfg, "gbapersoontab_csv", "sep", year)
    kwargs = dict(
        has_header = True,
        columns = ["RINPERSOON"],
        separator = sep
    )
    # some files have a different encoding
    encoding = dataset_option(cfg, "gbapersoontab_csv", "encoding", year)
    if encoding is not None:
        kwargs["encoding"] = encoding

    # get node dataframe for year
    print("kwargs")
    print(json.dumps(kwargs,indent=4))
    node_df = pl.read_csv(node_file,**kwargs)
    print("dataframe shape",node_df.shape)
    # merge nodes to set
    node_set = node_set + list(set(node_df["RINPERSOON"]).difference(set(node_set)))
    print("length set",len(node_set))

# create dataframe from merged node set
merged_node_df = pd.DataFrame(node_set)
merged_node_df.reset_index(inplace=True)
# RINPERSOON to label, index to integer ID
merged_node_df.rename(columns = {0:"label","index":"id"},inplace=True)
merged_node_df.head()

# save
print(f"Saving merged dataframe to " + os.path.join(working_folder,"temp",f"merged_node_mapping_{start_year}_{end_year}.csv.gz")+"...")
merged_node_df.to_csv(os.path.join(working_folder,"temp",f"merged_node_mapping_{start_year}_{end_year}.csv.gz"),header=True,index=False,compression="gzip")
print("Done.")
