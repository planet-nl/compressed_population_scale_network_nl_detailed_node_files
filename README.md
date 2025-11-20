# Population-Scale Network Node Files - Detailed Processing Pipeline

## Author
Eszter Bokanyi (e.bokanyi@liacs.leidenuniv.nl)  
Created: 2025-05-26

## Overview

This repository contains a comprehensive data processing pipeline that generates detailed node attribute files for yearly population-scale social network analysis in the Microdata RA environment of Statistics Netherlands. The pipeline processes CBS Microdata to create enriched node dataframes containing demographic, socioeconomic, educational, and geographic information for all individuals registered in the GBA (Gemeentelijke Basisadministratie) between configurable start and end years.

The pipeline combines multiple data sources from CBS Microdata to generate yearly node attribute tables that can be used with `mlnlib` multilayer network objects for longitudinal network analysis.

## Pipeline Architecture

### Workflow Overview

The pipeline follows a sequential processing approach:

1. **Merge all person IDs** across years to create a unified node mapping
2. **For each year**, process:
   - Base demographic information
   - Income data (2011+)
   - Education levels
   - Geographic location (up to buurt level)
   - Buurt metadata
   - Gemeente metadata
3. **Combine all attributes** into a single comprehensive node file per year
4. **Clean up** temporary files

### Output Structure

```
working_folder/
├── yearly_node_files/          # Final output files
│   └── nodes_start_{start_year}_end_{end_year}_year_{year}.csv.gz
├── codebook/                   # Metadata codebooks
│   └── gemeente_metadata_codebook_{year}.json
├── temp/                       # Temporary intermediate files (deleted after completion)
└── log files                   # Execution logs with timestamps
```

## Scripts Overview

### 00_run_all.sh
**Master orchestration script**

- Coordinates the entire pipeline execution
- Sets up directory structure (temp, codebook, yearly_node_files)
- Processes years sequentially from `start_year` to `end_year`
- Generates timestamped log files for debugging
- Cleans up temporary files upon completion

**Configuration:**
- `working_folder`: Base directory for all operations (e.g., `H:\ODISSEI_portal_C`)
- `start_year`: First year to process (e.g., 2009)
- `end_year`: Last year to process (e.g., 2023)

### 01_nodes_merged_nodelist.py
**Creates unified node ID mapping**

**Purpose:** Generates a master list of all unique RINPERSOON identifiers that appear in any GBA file between start_year and end_year, assigning each a unique integer ID for node-aligned network encoding for longitudinal analysis.

**Input:**
- GBAPERSOONTAB files for each year (CSV format)
- `files_per_year.json` configuration file to find the correct file paths, separators, and encodings for GBAPERSOONTAB files

**Output:**
- `temp/merged_node_mapping_{start_year}_{end_year}.csv.gz`
  - Columns: `id` (integer), `label` (RINPERSOON)

**Key Features:**
- Handles varying file encodings and separators across years
- Uses Polars for efficient large-scale data processing
- Creates union of all person IDs

### 02_nodes_base_files.py
**Extracts base demographic information**

**Purpose:** Creates yearly node dataframes with core demographic attributes and activity flags.

**Input:**
- `merged_node_mapping_{start_year}_{end_year}.csv.gz` (from script 01)
- GBAPERSOONTAB (current year)
- GBAADRESOBJECTBUS (previous year)
- GBAOVERLIJDENTAB (latest available)
- KINDOUDERTAB (parent-child relationships)

**Output:**
- `temp/base_start_{start_year}_end_{end_year}_year_{year}.csv.gz`

**Columns:**
- `label`: RINPERSOON identifier
- `id`: Integer ID from merged mapping
- `active`: Boolean flag (True if registered and alive on Jan 1 of given year)
- `gender`: Gender code
- `birth_year`: Year of birth
- `migrant_generation`: Migrant generation classification
- `number_of_parents_from_abroad`: Count (0-2)
- `missing_mother`: Flag for missing mother record
- `missing_father`: Flag for missing father record

**Logic:**
- Determines "active" status based on:
  - Having a registered address on Dec 31 of previous year
  - NOT being deceased by Jan 1 of target year
- Links parent information from KINDOUDERTAB

### 03_nodes_income.py
**Calculates household and individual income** (years 2011+)

**Purpose:** Enriches nodes with household income, individual income, and percentile rankings using household network analysis. Household income is assigned based on connected components in the household network, and NOT the CBS economic main earner assignment and household construction method to align any potential future network analysis.

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
- `household_income_percentile`: Household income percentile (1-100)
- `individual_income_gross`: Individual gross income
- `individual_income_percentile`: Individual income percentile
- `socioeconomic_situation`: Socioeconomic classification

**Key Features:**
- Uses graph theory (connected components) to identify households
- Handles households with no earners, single earners, and multiple earners
- For multiple earner households: averages income and maps to percentile
- Filters out institutional households and unknown income values

### 04_nodes_education.py
**Determines highest education level**

**Purpose:** Maps education codes to standardized 4-level classification system.

**Input:**
- HOOGSTEOPLTAB files (varies by year)
- Education code conversion tables, varying sources depending on year (OPLEIDINGSNRREFV34.SAV, CTOREFV12.sav)

**Output:**
- `temp/education_{year}.csv.gz`

**Columns:**
- `label`: RINPERSOON
- `educ_level`: Education level (single character code)
- `educ_weight`: Weight for education record

**Key Features:**
- Handles different education coding systems across years:
  - 2009-2012: Uses OPLNRHB codes with conversion
  - 2013-2018: Uses OPLNIVSOI2016AGG4HBMETNIRWO
  - 2019+: Uses OPLNIVSOI2021AGG4HBmetNIRWO
- Converts all to standardized 4-level aggregation

### 05_nodes_location.py
**Links geographic location to buurt level**

**Purpose:** Identifies the buurt, wijk, and gemeente of each person's registered address on Jan 1.

**Input:**
- GBAADRESOBJECTBUS (addresses)
- VSLGWBTAB (address to buurt mapping)

**Output:**
- `temp/location_{year}.csv.gz`

**Columns:**
- `label`: RINPERSOON
- `buurt_code`: Buurt code (e.g., BU00000101)
- `wijk_code`: Wijk code (e.g., WK000001)
- `gemeente_code`: Municipality code (e.g., GM0001)
- `household_change_year`: Year of most recent address change

**Key Features:**
- Filters addresses valid on Jan 1 of target year
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
- `buurt_name`: Buurt name
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

**Purpose:** Combines all individual attribute files into a single comprehensive node file per year.

**Input:**
- All temporary files from scripts 02-07

**Output:**
- `yearly_node_files/nodes_start_{start_year}_end_{end_year}_year_{year}.csv.gz`

**Final Schema:**
- **Identity:** label, id
- **Status:** active
- **Demographics:** gender, birth_year, migrant_generation, number_of_parents_from_abroad, missing_mother, missing_father
- **Income (2011+):** household_income, household_income_percentile, individual_income_gross, individual_income_percentile, socioeconomic_situation
- **Education:** educ_level, educ_weight
- **Location:** buurt_code, wijk_code, gemeente_code, household_change_year
- **Buurt metadata:** buurt_centroid_x, buurt_centroid_y, buurt_centroid_lat, buurt_centroid_lon, buurt_eff_r
- **Municipality metadata:** landsdeel, provincie, coropgebied, stedgem

**Key Features:**
- Left joins ensure all nodes from merged mapping are present
- Proper type casting for all columns
- Conditional handling of income data (only for years 2011+)

## Configuration Files

### files_per_year.json
Contains year-specific file paths and parameters:
- `node_files`: Custom file paths for non-standard GBAPERSOONTAB files
- `node_sep`: CSV separators (varies: `,`, `;`, `\t`)
- `node_encoding`: Special character encodings when needed

### layers.csv
Defines network layer structure for multilayer network analysis:
- Household connections (401, 402)
- School connections (501-506)
- Neighbor connections (101, 102)
- Colleague connections (201)
- Family connections (301-308+)

## Data Sources

### Primary CBS Microdata Files

| File | Description | Frequency |
|------|-------------|-----------|
| GBAPERSOONTAB | Person demographics | Yearly |
| GBAADRESOBJECTBUS | Address registrations | Yearly |
| GBAOVERLIJDENTAB | Death records | Updated yearly (cumulative) |
| KINDOUDERTAB | Parent-child relationships | Single file (2024) |
| HUISGENOTENNETWERKTAB | Household networks | Yearly |
| INHATAB | Household income | Yearly (2011+) |
| INPATAB | Individual income | Yearly (2011+) |
| HOOGSTEOPLTAB | Education levels | Yearly |
| VSLGWBTAB | Address-buurt mapping | Updated |

### Utility Files

| File | Description |
|------|-------------|
| GIN files | Administrative region mappings |
| GIS shapefiles | Buurt/gemeente boundaries |
| Education conversion tables | Standardization of education codes |

## System Requirements

### Python Environment
- Python 3.x
- Required packages:
  - `pandas`
  - `polars`
  - `numpy`
  - `scipy`
  - `geopandas`
  - `pyreadstat`
  - `mlnlib` (for network analysis)

### Computational Requirements
- Large memory capacity (population-scale data)
- Access to CBS Microdata remote access environment
- Sufficient disk space for temporary files

## Usage

### Full Pipeline Execution

```bash
# Edit configuration in 00_run_all.sh
working_folder="H:\\ODISSEI_portal_C"
start_year=2009
end_year=2023

# Execute
bash 00_run_all.sh
```

### Individual Script Execution

```bash
# Create merged node mapping
python 01_nodes_merged_nodelist.py 2009 2023 /h/ODISSEI_portal_C

# Process single year
python 02_nodes_base_files.py 2009 2023 2015 /h/ODISSEI_portal_C
python 03_nodes_income.py 2009 2023 2015 /h/ODISSEI_portal_C  # Only for 2011+
python 04_nodes_education.py 2015 /h/ODISSEI_portal_C
python 05_nodes_location.py 2015 /h/ODISSEI_portal_C
python 06_buurt_metadata.py 2015 /h/ODISSEI_portal_C
python 07_gemeente_metadata.py 2015 /h/ODISSEI_portal_C
python 08_combined_nodelists.py 2009 2023 2015 /h/ODISSEI_portal_C

# Process multiple years with loop
for year in $(seq 2009 2023); do
    python 02_nodes_base_files.py 2009 2023 $year /h/ODISSEI_portal_C
    # ... other scripts
done
```

## Output Files

### Final Node Files
- Location: `{working_folder}/yearly_node_files/`
- Format: `nodes_start_{start_year}_end_{end_year}_year_{year}.csv.gz`
- Compression: gzip
- Structure: One row per person in merged node mapping

### Codebook Files
- Location: `{working_folder}/codebook/`
- Format: JSON
- Content: Mappings from codes to human-readable labels

### Log Files
- Location: `{working_folder}/`
- Format: 
  - `log_messages_{timestamp}.log` - Standard output
  - `log_error_{timestamp}.log` - Error messages

## Important Notes

### Data Privacy
- All scripts operate within CBS Microdata secure environment
- RINPERSOON identifiers are pseudonymized person IDs
- Output files should remain within secure environment

### Year-Specific Considerations
- **Income data**: Only available from 2011 onwards
- **File formats**: Vary by year (CSV, SAV, XLSX)
- **Education coding**: Different systems in different periods
- **Administrative boundaries**: Buurt/gemeente codes change over time

### Active Node Definition
A person is considered "active" in year Y if they:
1. Were registered at an address on Dec 31 of year Y-1
2. Were NOT deceased by Jan 1 of year Y

### Known Limitations
- Household income assignments may have temporal mismatches
- Some households have no identified main earners
- Education level coding changes require careful interpretation
- Geographic centroids are approximations (use `buurt_eff_r` as error metric)

## Troubleshooting

### Common Issues

**Missing files for specific years:**
- Check `files_per_year.json` for custom paths
- Verify file naming conventions in CBS Microdata
- File naming may alter in Microdata RA environment over time

**Encoding errors:**
- Add year-specific encodings to `files_per_year.json`
- Check file format (some years use different separators)

**Geometric errors in buurt processing:**
- Ensure shapefile integrity
- Check CRS definitions for coordinate transformations

## Contact

For questions or issues, contact:
- Eszter Bokanyi: e.bokanyi@liacs.leidenuniv.nl