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

Inputs:
-------
GBAPERSOONTAB files each year between start_year and end_year in CSV format
files_per_year.json filename and file reading dictionary in same folder

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

# capturing script arguments
start_year = int(sys.argv[1])
end_year = int(sys.argv[2])
working_folder = sys.argv[3]

# opening configuration file to capture custom file paths
files_per_year = json.load(open(os.path.join(working_folder,"src","files_per_year.json")))

# creating list of years between start_year and end_year
year_list = list(range(start_year,end_year+1))

# set of nodes
node_set = list()

for year in year_list:
    print("YEAR",year)
    # default CSV file name for GBAPERSOONTAB
    node_file = f"G:\Bevolking\GBAPERSOONTAB\\{year}\geconverteerde data\GBAPERSOON{year}TABV1_csv.csv"
    # if default name does not exist, read filepath from config file
    if not os.path.exists(node_file):
        node_file = files_per_year["node_files"][str(year)][0]
    print(f"Reading {node_file}...")
    sep = files_per_year["node_sep"][str(year)]
    kwargs = dict(
        has_header = True,
        columns = ["RINPERSOON"],
        separator = sep
    )
    # some files have a different encoding
    if str(year) in files_per_year["node_encoding"]:
        kwargs["encoding"] = files_per_year["node_encoding"][str(year)]

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
print(f"Saving merged dataframe to " + os.path.join(working_folder,f"merged_node_mapping_{start_year}_{end_year}.csv.gz..."))
merged_node_df.to_csv(os.path.join(working_folder,"temp",f"merged_node_mapping_{start_year}_{end_year}.csv.gz"),header=True,index=False,compression="gzip")
print("Done.")
