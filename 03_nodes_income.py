"""
Author: Eszter, 2024.03.26.

This script calculates household and individual income and income percentiles.

Input:
------
HUISHOUDENNETWERKTAB files, e.g.
    "G:\\Bevolking\\HUISGENOTENNETWERKTAB\\HUISGENOTENNETWERKTAB{year}V1.csv"
INHATAB files, e.g.
    "G:\InkomenBestedingen\INHATAB\INHA{year}TABVx.sav"
INPATAB files, e.g.
    "G:\InkomenBestedingen\INPATAB\INPA{year}TABVx.sav"

Output:
-------
    * node dataframe with household and individual income and percentile along to node labels (RINPERSOON)


Arguments:
----------
    start_year
    end_year
    year
    output_folder

Usage:
------
    /c/mambaforge/envs/9629/python.exe /h/ebyi/02_nodes_income.py 2009 2023 2022 .

Bash script:
------------

# income data starts in 2011
for year in `seq 2022 2023`
do
    /c/mambaforge/envs/9629/python.exe /h/ebyi/02_nodes_income.py $year
done
"""
# 
import pandas as pd
import polars as pl
import numpy as np

import sys
sys.stdout.reconfigure(encoding="utf-8")


from scipy.sparse.csgraph import connected_components
from mlnlib.mln import MultiLayerNetwork
from scipy.sparse import csr_matrix

import os
import re

# argument parsing
start_year = int(sys.argv[1])
end_year = int(sys.argv[2])
year = int(sys.argv[3])
base_node_data_folder = sys.argv[4]
output_folder = base_node_data_folder

# inpatab fileliest
inpatab_files = {}
inpatab_path = "G:\\InkomenBestedingen\\INPATAB\\"
for f in os.listdir(inpatab_path):
    year_match = re.findall("[0-9]{4,4}",f)
    if len(year_match)==1 and "INPA" in f:
        inpatab_files[int(year_match[0])] = os.path.join(inpatab_path,f)

# inhatab filelist
inhatab_files = {}
inhatab_path = "G:\\InkomenBestedingen\\INHATAB\\"
for f in os.listdir(inhatab_path):
    year_match = re.findall("[0-9]{4,4}",f)
    if len(year_match)==1  and  "INHA" in f:
        inhatab_files[int(year_match[0])] = os.path.join(inhatab_path,f)



inhatab_file = inhatab_files[year]
inpatab_file = inpatab_files[year]
network_path = f"G:\\BEVOLKING\\HUISGENOTENNETWERKTAB\\HUISGENOTENNETWERK{year}TABV1.csv"
nodes_path = f"{base_node_data_folder}\\temp\\base_start_{start_year}_end_{end_year}_year_{year}.csv.gz"

print(f"Loading household connections from {network_path}...")
# load nodes
nodes = pl.read_csv(nodes_path)
N = max(nodes["id"])+1
print(nodes.head())
print(N)


edgelist_rename_cols = {
    "RINPERSOON" : "source",
    "RINPERSOONRELATIE" : "target",
    "RELATIE" : "layer"
}
# load household edges
edgelist = (
    pl.read_csv(network_path,separator=";")
        .select([pl.col(c) for c in edgelist_rename_cols])\
        .rename(edgelist_rename_cols)
        .filter(pl.col("layer")==401) # non-institutional household members
        .with_columns(
                    pl.col("source").cast(pl.Int64),
                    pl.col("target").cast(pl.Int64)
        )
)
print(edgelist.head())

# convert to sparse edgelist
ij = (
    edgelist
        .join(nodes.select(pl.col("label"),pl.col("id")),left_on="source",right_on="label",how="left").rename({"id":"i"})
        .join(nodes.select(pl.col("label"),pl.col("id")),left_on="target",right_on="label",how="left").rename({"id":"j"})
        .filter(
            (~pl.col("i").is_null())&\
            (~pl.col("j").is_null())
        )\
        .select(
            pl.col("i"),
            pl.col("j")
        )
)
# force undirected connections
ij = pl.concat([
    ij,
    ij.rename({"i":"j","j":"i"}).select(pl.col("i"),pl.col("j"))
])
print(ij.head())

i = ij["i"].to_list()
j = ij["j"].to_list()

# create adjacency matrix
A = csr_matrix((np.ones(len(i)),(i,j)), shape=(N,N), dtype = np.uint64)
# if edges have accidentally been listed twice bc of symmetrization, drop
A = csr_matrix(A>0, dtype=np.uint64)

del edgelist, ij

# load layers
layers = pd.read_csv(os.path.join(output_folder,"src","layers.csv"),index_col=None,header=0)

# create network object
households = MultiLayerNetwork(
    nodes = nodes,
    edges = A,
    layers = layers
)
print(households)

# INHATAB
print(f"Reading INHATAB file {inhatab_file} for year {year}...")
inhatab_cols = ["RINPERSOONHKW","INHP100HGEST","INHGESTINKH"]
household_incomes_nodes = pd.read_spss(
    inhatab_file,
    usecols=inhatab_cols,
    convert_categoricals=False # if this is True, then values such as gender are set to their string values
)
household_incomes_nodes.columns = ["label_hkw","income_value","income_percentile"]
# filtering households with unknown income, and institutional households
household_incomes_nodes = household_incomes_nodes[~((household_incomes_nodes["income_value"]>9.9999e9)|(household_incomes_nodes["income_percentile"]<1))]
print(household_incomes_nodes.head())
print("Done.")

# INPATAB
print(f"Reading INPATAB file {inpatab_file} for year {year}...")
inpatab_cols = ["RINPERSOON","INPP100PBRUT","INPBELI","INPSECJ"]
individual_incomes_nodes = pd.read_spss(
    inpatab_file,
    usecols=inpatab_cols,
    convert_categoricals=False # if this is True, then values such as gender are set to their string values
)
individual_incomes_nodes.columns = ["label","individual_income_gross","individual_income_percentile","socioeconomic_situation"]
individual_incomes_nodes["label"] = individual_incomes_nodes["label"].map(int)
print(individual_incomes_nodes.head())
print("Done.")

# label to income/percentile dicts
income_map = dict(zip(household_incomes_nodes["label_hkw"].map(int),household_incomes_nodes["income_value"]))
percentile_map = dict(zip(household_incomes_nodes["label_hkw"].map(int),household_incomes_nodes["income_percentile"]))
# set of main earners in households
hkw = set(household_incomes_nodes["label_hkw"].map(int))

print("Getting connected components...")
# connected components of household layer
cc = connected_components(households.A.sign())
households.nodes = households.nodes.to_pandas()

# adding component label to node dataframe
households.nodes["household_component"] = cc[1]
# labelling main earners in node dataframe
households.nodes["is_hkw"] = households.nodes["label"].isin(hkw)
print("Done.")

# sanity check: household size vs number of main earners from income file
size_vs_earners = pd.DataFrame(households.nodes\
    [households.nodes["active"]]\
    .groupby("household_component")\
    .agg({"label":"count","is_hkw":"sum"})\
    .value_counts())
size_vs_earners.reset_index(inplace=True)
size_vs_earners.rename(columns = {"is_hkw":"earners", "label":"size"},inplace=True)
# no earners
# could be the mismatch in time between network amnd income file!
print("Households counts per household size with no main earners")
print(size_vs_earners[size_vs_earners["earners"]==0])

# 
# multiple earners
print("Households counts per household size with multiple main earners")
print(size_vs_earners[size_vs_earners["earners"]>1])

# 
# separating three types of households

earner_count = households.nodes\
    [households.nodes["active"]]\
    .groupby("household_component")\
    .agg({"label":"count","is_hkw":"sum","label": lambda x: list(x)})

no_earner_households = earner_count\
    .query("is_hkw==0")

single_earner_households = earner_count\
    .query("is_hkw==1")

multiple_earner_households = earner_count\
    .query("is_hkw>=2")


# 
print("Percentage of no earner households our of all households")
print(round(100*no_earner_households.shape[0]/len(np.unique(cc[1][households.nodes["active"]])),1))

# 
# creating average income for multiple earner households
multiple_earner_households["avg_income"] = \
    multiple_earner_households["label"].map(lambda l: np.mean([income_map[elem] for elem in l if elem in income_map]))
# what is the minimum income in each percentile bin?
percentile_lookup = \
    household_incomes_nodes\
        .groupby("income_percentile")\
        [["income_value"]]\
        .min()\
        .reset_index()
multiple_earner_households.reset_index(inplace=True)

# looking up percentile corresponding to average income in multiple earner households
multiple_earner_households["avg_percentile"] = percentile_lookup["income_percentile"]\
    [np.digitize(
        multiple_earner_households["avg_income"],\
        percentile_lookup["income_value"][1:]
    )].tolist()

# 
# getting income and percentaile for single earner households based on the data for the single earner
single_earner_households["income"] = single_earner_households["label"].map(lambda l: [income_map[elem] for elem in l if elem in income_map][0])
single_earner_households["percentile"] = single_earner_households["label"].map(lambda l: [percentile_map[elem] for elem in l if elem in percentile_map][0])

# 
# household to income and percentile mappers based on the above two dataframes
household_to_income = {
    **dict(zip(single_earner_households.index,single_earner_households.income)),
    **dict(zip(multiple_earner_households["household_component"],multiple_earner_households["avg_income"]))
}
household_to_percentile = {
    **dict(zip(single_earner_households.index,single_earner_households.percentile)),
    **dict(zip(multiple_earner_households["household_component"],multiple_earner_households["avg_percentile"]))
}

# 
# adding household incomes for all members of the household in the node dataframe
households.nodes["income"] = households.nodes["household_component"]\
.map(lambda c: household_to_income.get(c,-1) if c is not None else None)

# 
# adding household income percentiles for all members of the household in the node dataframe
households.nodes["percentile"] = households.nodes["household_component"]\
.map(lambda c: household_to_percentile.get(c,-1) if c is not None else None)

# 
inactives = ~households.nodes["active"]
households.nodes["household_component"][inactives] = None
households.nodes["income"][inactives] = None
households.nodes["percentile"][inactives] = None
households.nodes["is_hkw"][inactives] = None

# 
output = households.nodes[[
    "label",
    "income",
    "percentile"
]].rename(columns = {"income":"household_income","percentile":"household_income_percentile"})

print("Joining individual income...")
output.set_index("label",inplace=True)
individual_incomes_nodes.set_index("label",inplace=True)
output = output.join(individual_incomes_nodes,how="left")
output.reset_index(inplace=True)
print(output.head())
print("Done.")

print(f"Saving results to {output_folder}...")
output.to_csv(os.path.join(output_folder,"temp",f"income_{year}.csv.gz"),index=False,header=True,compression="gzip")
print("Done.")



