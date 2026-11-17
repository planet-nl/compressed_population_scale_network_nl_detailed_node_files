# Detailed Node Attributes for Compressed Population-scale Network Netherlands 2009-2023

Scripts that build yearly node attribute tables (demographics, income, education, place of residence and area metadata) for every person in the population-scale networks of the Netherlands, in the CBS Microdata Remote Access (RA) environment.

| | |
|---|---|
| **Dataset** | Detailed Node Attributes for Compressed Population-scale Network Netherlands 2009-2023 |
| **DOI** | forthcoming |
| **Content** | One folder per year: node table (`nodes.csv.gz`, 29 columns, one row per person in the node set of the networks) and codebook (`codebook.json`) |
| **Years** | 2009–2023 (income from 2011) |
| **Access** | CBS Microdata environment, CBS project 9629 |

The first 9 columns (script 02) are also the `nodes.csv.gz` of the two network datasets, and `id` is the row/column index of their adjacency matrices. This is one of three related datasets, see [Related datasets](#related-datasets).


## Overview

The pipeline combines several CBS Microdata sources into one node attribute table per year for all individuals registered in the GBA between a configurable start and end year. The tables can be used as node tables of `mlnlib` multilayer network objects or on their own for longitudinal person-level analysis.

### Workflow

1. **Merge all person IDs** across years to create a unified node mapping (or reuse an existing one)
2. **For each year**, process:
   - Base demographic information
   - Income data (2011+)
   - Education levels
   - Geographic location (up to buurt level)
   - Buurt metadata
   - Gemeente metadata
3. **Combine all attributes** into a single node file per year
4. **Clean up** temporary files
5. **Reorganize** the output into one folder per year (script 09)

### Output structure

Scripts 01–08 write into the working folder:

```
working_folder/
├── src/                        # Scripts, config.json, pipeline_config.py, layers.csv
├── yearly_node_files/          # Node files
│   └── nodes_start_{start_year}_end_{end_year}_year_{year}.csv.gz
├── codebook/                   # Metadata codebooks
│   └── gemeente_metadata_codebook_{year}.json
├── temp/                       # Temporary intermediate files (deleted after completion)
└── log files                   # log_messages_{timestamp}.log, log_error_{timestamp}.log
```

`09_reorganize_output.py` then copies the node files and codebooks into the layout of the dataset, one folder per year like the two network datasets:

```
dataset_folder/
├── 2009/
│   ├── nodes.csv.gz            # Node table of 2009
│   └── codebook.json           # Municipality metadata codebook of 2009
├── ...
└── 2023/
    ├── nodes.csv.gz
    └── codebook.json
```


## Data format

### nodes.csv.gz

- Gzip-compressed CSV, one row per person in the merged node mapping, sorted by `id`
- `id` is the row/column index in the adjacency matrices (`edges.npz`) of the network datasets of the same year

Columns:

- **Identity:** `label`, `id`
- **Status:** `active`
- **Demographics:** `gender`, `birth_year`, `migrant_generation`, `number_of_parents_from_abroad`, `missing_mother`, `missing_father`
- **Income (2011+):** `household_income`, `household_income_percentile`, `individual_income_gross`, `individual_income_percentile`, `socioeconomic_situation`
- **Education:** `educ_weight`, `educ_level`
- **Location:** `household_change_year`, `gemeente_code`, `wijk_code`, `buurt_code`
- **Buurt metadata:** `buurt_centroid_x`, `buurt_centroid_y`, `buurt_centroid_lat`, `buurt_centroid_lon`, `buurt_eff_r`
- **Municipality metadata:** `landsdeel`, `provincie`, `coropgebied`, `stedgem`

The columns are described per script below.

### codebook.json

- JSON object with one entry per municipality metadata column (`landsdeel`, `provincie`, `coropgebied`, `stedgem`), mapping each code to its name in that year


## Scripts

### 00_run_all.sh
**Master orchestration script**

- Coordinates the entire pipeline execution
- Sets up directory structure (temp, codebook, yearly_node_files)
- Processes years sequentially from `start_year` to `end_year`
- Copies the results into one folder per year in `dataset_folder` (script 09)
- Generates timestamped log files for debugging
- Cleans up temporary files upon completion

**Configuration:**
- `working_folder`: Base directory for all operations (e.g., `H:\ODISSEI_portal_C_work`)
- `dataset_folder`: Output folder of the dataset, one subfolder per year (e.g., `H:\ODISSEI_portal_C`); keep it outside `working_folder`
- `start_year`: First year to process (e.g., 2009)
- `end_year`: Last year to process (e.g., 2023)
- Everything else (file paths, reference date, first income year) is in `src/config.json`, see [Configuration](#configuration)

Paths are joined with `os.path.join`, so the scripts run both in the CBS RA (Windows) and on Linux/macOS.

### 01_nodes_merged_nodelist.py
**Creates unified node ID mapping**

**Purpose:** Generates a master list of all unique RINPERSOON identifiers that appear in any GBA file between start_year and end_year, assigning each a unique integer ID for node-aligned network encoding for longitudinal analysis.

**Input:**
- GBAPERSOONTAB files for each year (CSV format)
- `config.json` to find the correct file paths, separators, and encodings for GBAPERSOONTAB files
- Optional 4th argument: path to an existing `id`/`label` csv.gz mapping, which is saved as the mapping instead of building a new one (to keep ids aligned with existing edge files)

**Output:**
- `temp/merged_node_mapping_{start_year}_{end_year}.csv.gz`
  - Columns: `id` (integer), `label` (RINPERSOON)

**Key Features:**
- Handles varying file encodings and separators across years
- Uses Polars for efficient large-scale data processing
- Creates union of all person IDs

### 02_nodes_base_files.py
**Extracts base demographic information**

**Purpose:** Creates yearly node dataframes with core demographic attributes and activity flags. Its output is also the `nodes.csv.gz` of the two network datasets.

**Input:**
- `merged_node_mapping_{start_year}_{end_year}.csv.gz` (from script 01)
- GBAPERSOONTAB (current year)
- GBAADRESOBJECTBUS (latest available csv, cumulative)
- GBAOVERLIJDENTAB (latest available, cumulative)
- KINDOUDERTAB (parent-child relationships, latest available, cumulative)

**Output:**
- `temp/base_start_{start_year}_end_{end_year}_year_{year}.csv.gz`

**Columns:**
- `label`: RINPERSOON identifier
- `id`: Integer ID from merged mapping
- `active`: Boolean flag (True if registered at an address on the reference date and alive on Jan 1 of the given year)
- `gender`: Gender code
- `birth_year`: Year of birth
- `migrant_generation`: Migrant generation classification
- `number_of_parents_from_abroad`: Count (0-2)
- `missing_mother`: 1 if no mother is linked in KINDOUDERTAB (including people absent from KINDOUDERTAB), else 0
- `missing_father`: 1 if no father is linked in KINDOUDERTAB (including people absent from KINDOUDERTAB), else 0

**Logic:**
- Determines "active" status based on:
  - Having a registered address on the reference date (Jan 1 of the target year, see `config.json`)
  - NOT being deceased by Jan 1 of target year
- Links parent information from KINDOUDERTAB; persons without any record there get `missing_mother = missing_father = 1`
- The address filter is shared with script 05 (`pipeline_config.py`)

### 03_nodes_income.py
**Calculates household and individual income** (years 2011+)

**Purpose:** Enriches nodes with household income, individual income, and percentile rankings using household network analysis. Household income is assigned based on connected components in the household network, and NOT the CBS economic main earner assignment and household construction method, to align with network analysis.

**Input:**
- HUISGENOTENNETWERKTAB (household network files)
- INHATAB (household income)
- INPATAB (individual income)
- Base node file from script 02

**Output:**
- `temp/income_{year}.csv.gz`

**Columns:**
- `label`: RINPERSOON
- `household_income`: Household income value
- `household_income_percentile`: Household income percentile (1-100), empty for households without an identified main earner
- `individual_income_gross`: Individual gross income
- `individual_income_percentile`: Individual income percentile (CBS value from INPATAB)
- `socioeconomic_situation`: Socioeconomic classification

**Key Features:**
- Uses graph theory (connected components) to identify households
- Handles households with no earners, single earners, and multiple earners
- For multiple earner households: averages income of the main earners
- Households with no identified main earner get `household_income = -1` and an empty percentile
- Percentiles are recomputed over active persons in households with an identified main earner (person-weighted)
- Filters out institutional households and unknown income values

### 04_nodes_education.py
**Determines highest education level**

**Purpose:** Harmonizes the highest attained education level across the coding systems used in different years.

**Input:**
- HOOGSTEOPLTAB files (varies by year)
- Education code conversion tables for 2009-2012 only (OPLEIDINGSNRREFV34.SAV, CTOREFV13.sav)

**Output:**
- `temp/education_{year}.csv.gz`

**Columns:**
- `label`: RINPERSOON
- `educ_weight`: Weight for education record
- `educ_level`: Education level, the first digit of the aggregated SOI education level code (1 = low, 2 = middle, 3 = high; see the HOOGSTEOPLTAB documentation for other codes)

**Key Features:**
- Handles different education coding systems across years:
  - 2009-2012: Uses OPLNRHB codes with conversion
  - 2013-2018: Uses OPLNIVSOI2016AGG4HBMETNIRWO
  - 2019+: Uses OPLNIVSOI2021AGG4HBmetNIRWO
- Takes the first digit of the aggregated (AGG4) code in all periods, so the levels are comparable across years

### 05_nodes_location.py
**Links geographic location to buurt level**

**Purpose:** Identifies the buurt, wijk, and gemeente of each person's registered address on the reference date (Jan 1 of the target year, see `config.json`).

**Input:**
- GBAADRESOBJECTBUS (addresses, latest available csv)
- VSLGWBTAB (address to buurt mapping, column `bc{year}`)

**Output:**
- `temp/location_{year}.csv.gz`

**Columns:**
- `label`: RINPERSOON
- `household_change_year`: Year of most recent address change
- `gemeente_code`: Municipality code (e.g., GM0001)
- `wijk_code`: Wijk code (e.g., WK000001)
- `buurt_code`: Buurt code (e.g., BU00000101)

**Key Features:**
- Filters addresses valid on the reference date (Jan 1 of the target year, same as in script 02)
- Generates hierarchical location codes from 8-digit buurt codes
- Tracks year of last household/address change

### 06_buurt_metadata.py
**Extracts buurt geographic metadata**

**Purpose:** Generates geographic information (centroids, effective radius) for each buurt using GIS shapefiles.

**Input:**
- GIS shapefiles from `K:\Utilities\Tools\GISHulpbestanden\Gemeentewijkbuurt\{year}\`

**Output:**
- `temp/buurt_metadata_{year}.csv.gz`

**Columns:**
- `buurt_code`: Buurt identifier
- `buurt_name`: Buurt name (not included in the final node files)
- `buurt_centroid_x`: X coordinate (Amersfoort/RD projection)
- `buurt_centroid_y`: Y coordinate (Amersfoort/RD projection)
- `buurt_centroid_lat`: Latitude (WGS84)
- `buurt_centroid_lon`: Longitude (WGS84)
- `buurt_eff_r`: Effective radius in meters (sqrt(area/π))

**Key Features:**
- Uses GeoPandas for spatial operations
- Handles varying shapefile naming conventions across years within the RA
- Repairs geometries and filters invalid entries
- Calculates centroids in both Amersfoort (EPSG:28992) and WGS84 (EPSG:4326)

### 07_gemeente_metadata.py
**Extracts municipality hierarchy metadata**

**Purpose:** Links each gemeente to higher-level administrative regions.

**Input:**
- GIN (Gebieden in Nederland) files from `K:\Utilities\HULPbestanden\GebiedeninNederland\`

**Output:**
- `temp/gemeente_metadata_{year}.csv.gz`
- `codebook/gemeente_metadata_codebook_{year}.json`

**Columns:**
- `gemeente_code`: Municipality code
- `landsdeel`: Regional division code
- `provincie`: Province code
- `coropgebied`: COROP region code
- `stedgem`: Urban classification code

**Key Features:**
- Handles multiple GIN file formats (SAV, DTA, XLSX, CSV)
- Adapts to changing column naming conventions across years within the RA
- Generates codebooks mapping codes to human-readable names
- Maintains consistency across different classification systems

### 08_combined_nodelists.py
**Merges all attribute files**

**Purpose:** Combines all individual attribute files into a single node file per year (see [Data format](#data-format)).

**Input:**
- All temporary files from scripts 02-07

**Output:**
- `yearly_node_files/nodes_start_{start_year}_end_{end_year}_year_{year}.csv.gz`

**Key Features:**
- Left joins ensure all nodes from merged mapping are present
- Proper type casting for all columns
- Conditional handling of income data (only for years 2011+)

### 09_reorganize_output.py
**Copies the output into one folder per year**

**Purpose:** Gives the dataset the same layout as the two network datasets, so that every year folder holds its own node table and codebook.

**Input:**
- `yearly_node_files/nodes_start_{start_year}_end_{end_year}_year_{year}.csv.gz` (from script 08)
- `codebook/gemeente_metadata_codebook_{year}.json` (from script 07)

**Output:**
- `{dataset_folder}/{year}/nodes.csv.gz`
- `{dataset_folder}/{year}/codebook.json`

**Key Features:**
- Copies the files, so the pipeline output stays unchanged; use a `dataset_folder` outside `{working_folder}`


## Configuration

### config.json
Central configuration read by all scripts through `pipeline_config.py` (both files, together with `layers.csv`, have to be in `{working_folder}/src/`):
- `settings`: `address_reference` (`previous_year_dec31` or `year_jan1`: the date on which addresses are looked up, both for the population in script 02 and the location in script 05; the published dataset uses `year_jan1`), `income_first_year`, `household_member_layer`, `layers_file`
- `education`: reference table files and the coding periods (which column holds the education level in which years, and whether it needs conversion)
- `datasets`: per dataset a default `path` (with `{year}`), `overrides` for years with different file names, and `sep` / `encoding` (with `sep_overrides` / `encoding_overrides` per year). Cumulative files (GBAOVERLIJDENTAB, KINDOUDERTAB, GBAADRESOBJECTBUS) have a single path to the latest version. INPATAB and INHATAB are found by scanning a directory.
- To add a year or a new file version, edit this file only. Places in the code changed to read from the config are marked with `CHG-2026-11`.

### layers.csv
The 33-layer definition of the network dataset (same file as in [compressed_population_scale_network_nl](https://github.com/planet-nl/compressed_population_scale_network_nl)). Script 03 only uses it to select the household-member layer (401):
- Household connections (401, 402)
- School connections (501-506)
- Neighbor connections (101, 102)
- Work connections (201)
- Family connections (301-322)


## Data sources

### Primary CBS Microdata files

| File | Description | Version used |
|------|-------------|-----------|
| GBAPERSOONTAB | Person demographics | Yearly |
| GBAADRESOBJECTBUS | Address registrations | Latest file (cumulative) |
| GBAOVERLIJDENTAB | Death records | Latest file (cumulative) |
| KINDOUDERTAB | Parent-child relationships | Latest file (cumulative) |
| HUISGENOTENNETWERKTAB | Household networks | Yearly (2011+) |
| INHATAB | Household income | Yearly (2011+) |
| INPATAB | Individual income | Yearly (2011+) |
| HOOGSTEOPLTAB | Education levels | Yearly |
| VSLGWBTAB | Address-buurt mapping | Latest file (one `bc{year}` column per year) |

### Utility files

| File | Description |
|------|-------------|
| GIN files | Administrative region mappings |
| GIS shapefiles | Buurt/gemeente boundaries |
| Education conversion tables | Standardization of education codes |


## Requirements

- Python 3.10+
- `pandas`, `polars`, `numpy`, `scipy`, `geopandas`, `pyreadstat`, `openpyxl` (script 07, GIN xlsx files), `mlnlib` (script 03)
- Access to the CBS Microdata RA environment, a large memory capacity and enough disk space for the temporary files

The published data were produced with `mlnlib` 0.2.39; 0.3.0 should work as well.

```bash
pip install pandas polars numpy scipy geopandas pyreadstat openpyxl mlnlib==0.2.39
```


## Usage

### Full pipeline

```bash
# Edit configuration in 00_run_all.sh
working_folder="H:\\ODISSEI_portal_C_work"
dataset_folder="H:\\ODISSEI_portal_C"
start_year=2009
end_year=2023

# Execute (scripts 01-09)
bash 00_run_all.sh
```

### Individual scripts

```bash
# Create merged node mapping (optionally reuse an existing id/label mapping as 4th argument)
python 01_nodes_merged_nodelist.py 2009 2023 /h/ODISSEI_portal_C_work [existing_mapping.csv.gz]

# Process single year
python 02_nodes_base_files.py 2009 2023 2015 /h/ODISSEI_portal_C_work
python 03_nodes_income.py 2009 2023 2015 /h/ODISSEI_portal_C_work  # Only for 2011+
python 04_nodes_education.py 2015 /h/ODISSEI_portal_C_work
python 05_nodes_location.py 2015 /h/ODISSEI_portal_C_work
python 06_buurt_metadata.py 2015 /h/ODISSEI_portal_C_work
python 07_gemeente_metadata.py 2015 /h/ODISSEI_portal_C_work
python 08_combined_nodelists.py 2009 2023 2015 /h/ODISSEI_portal_C_work

# One folder per year (after all years are done)
python 09_reorganize_output.py /h/ODISSEI_portal_C_work /h/ODISSEI_portal_C 2009 2023

# Process multiple years with loop
for year in $(seq 2009 2023); do
    python 02_nodes_base_files.py 2009 2023 $year /h/ODISSEI_portal_C_work
    # ... other scripts
done
```


## Repository structure

```
.
├── 00_run_all.sh               # Runs all steps for a range of years
├── 01_nodes_merged_nodelist.py # Unified id mapping
├── 02_nodes_base_files.py      # Base demographics and the active flag
├── 03_nodes_income.py          # Household and individual income (2011+)
├── 04_nodes_education.py       # Highest education level
├── 05_nodes_location.py        # Gemeente, wijk and buurt of the address
├── 06_buurt_metadata.py        # Buurt centroids and size
├── 07_gemeente_metadata.py     # Municipality regions and codebook
├── 08_combined_nodelists.py    # Joins everything into the yearly node file
├── 09_reorganize_output.py     # One folder per year: nodes.csv.gz, codebook.json
├── pipeline_config.py          # Reads config.json, shared helpers
├── config.json                 # File paths and settings
├── layers.csv                  # 33-layer definition of the network dataset
└── README.md
```

`00_run_all.sh` expects the scripts, `config.json`, `pipeline_config.py` and `layers.csv` in `{working_folder}/src/`.


## Related datasets

The three datasets cover 2009–2023 and share the same nodes: every person has the same `id` and `label` (RINPERSOON) in all of them, and the yearly population (`active`) is defined on 1 January. The `nodes.csv.gz` of the two network datasets holds the first 9 columns of the node attribute tables. All three are available in the CBS Microdata environment (CBS project 9629) and can be read with [`mlnlib`](https://github.com/planet-nl/mlnlib) ([PyPI](https://pypi.org/project/mlnlib/)).

| Dataset | Content | DOI | Code |
|---|---|---|---|
| Compressed Population-scale Network Netherlands 2009-2023 in MLN format | 33 layers, directed | [10.34894/8575OP](https://doi.org/10.34894/8575OP) | [compressed_population_scale_network_nl](https://github.com/planet-nl/compressed_population_scale_network_nl) |
| Simplified Compressed Population-scale Network Netherlands 2009-2023 in MLN format | 5 layers, undirected | forthcoming | [compressed_population_scale_network_nl_simplified](https://github.com/planet-nl/compressed_population_scale_network_nl_simplified) |
| Detailed Node Attributes for Compressed Population-scale Network Netherlands 2009-2023 | Yearly node attribute tables | forthcoming | [compressed_population_scale_network_nl_detailed_node_files](https://github.com/planet-nl/compressed_population_scale_network_nl_detailed_node_files) |


## How to cite

Please cite the dataset(s) you use:

> Bokányi, E., Kazmina, Y., Heemskerk, E. M., & Takes, F. W. (2025). *Compressed Population-scale Network Netherlands 2009-2023 in MLN format* [Data set]. ODISSEI Portal. https://doi.org/10.34894/8575OP

> Bokányi, E., Kazmina, Y., Heemskerk, E. M., & Takes, F. W. (2026). *Simplified Compressed Population-scale Network Netherlands 2009-2023 in MLN format* [Data set]. ODISSEI Portal. DOI forthcoming.

> Bokányi, E., Kazmina, Y., Heemskerk, E. M., & Takes, F. W. (2026). *Detailed Node Attributes for Compressed Population-scale Network Netherlands 2009-2023* [Data set]. ODISSEI Portal. DOI forthcoming.

and the paper describing the network:

> Bokányi, E., Heemskerk, E. M., & Takes, F. W. (2023). The anatomy of a population-scale social network. *Scientific Reports*, 13, 9209. https://doi.org/10.1038/s41598-023-36324-9


## Authors and contact

Eszter Bokányi (Leiden University), Yuliia Kazmina (University of Amsterdam), Eelke M. Heemskerk (University of Amsterdam), Frank W. Takes (Leiden University), and the PLANET-NL team (https://planetnl.org).

For questions, contact the PLANET-NL team: info@planetnl.org
