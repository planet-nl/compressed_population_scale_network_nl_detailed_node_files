"""
Copies the pipeline output into one folder per year, like the network datasets:

    {output_folder}/{year}/nodes.csv.gz
    {output_folder}/{year}/codebook.json

Usage: python 09_reorganize_output.py <working_folder> <output_folder> <start_year> <end_year>
"""
import os
import shutil
import sys

working_folder, output_folder = sys.argv[1], sys.argv[2]
start_year, end_year = int(sys.argv[3]), int(sys.argv[4])

for year in range(start_year, end_year + 1):
    year_folder = os.path.join(output_folder, str(year))
    os.makedirs(year_folder, exist_ok=True)
    shutil.copy2(
        os.path.join(working_folder, "yearly_node_files", f"nodes_start_{start_year}_end_{end_year}_year_{year}.csv.gz"),
        os.path.join(year_folder, "nodes.csv.gz"),
    )
    shutil.copy2(
        os.path.join(working_folder, "codebook", f"gemeente_metadata_codebook_{year}.json"),
        os.path.join(year_folder, "codebook.json"),
    )
    print(f"{year} done.")
