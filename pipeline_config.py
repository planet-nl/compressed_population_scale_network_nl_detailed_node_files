"""
Author: Eszter Bokanyi, e.bokanyi@liacs.leidenuniv.nl
Last modified: 2026.11.03

Shared helpers of the node file pipeline: reading config.json (file paths, separators, encodings,
reference dates), and reading the address table, so that scripts 02 and 05 use the same source
and the same reference date.

config.json is expected in {working_folder}/src/, next to the scripts. This module has to be
copied there too.
"""

import json
import os
import re


def load_config(working_folder):
    """Reads {working_folder}/src/config.json."""
    with open(os.path.join(working_folder, "src", "config.json"), encoding="utf-8") as f:
        return json.load(f)


def dataset_path(cfg, name, year=None):
    """
    Path of dataset `name` for `year`: the per-year override if there is one, otherwise the
    default path with {year} filled in. Datasets without {year} in their path (e.g. cumulative
    files) can be called without year.
    """
    ds = cfg["datasets"][name]
    if year is not None and str(year) in ds.get("overrides", {}):
        return ds["overrides"][str(year)]
    return ds["path"].format(year=year)


def dataset_option(cfg, name, option, year=None, default=None):
    """Per-year option (e.g. "sep", "encoding") of a dataset: `{option}_overrides`, else `{option}`, else default."""
    ds = cfg["datasets"][name]
    return ds.get(option + "_overrides", {}).get(str(year), ds.get(option, default))


def scan_year_files(cfg, name):
    """
    For datasets whose file name contains an unpredictable version part (INPATAB, INHATAB):
    scans the directory and returns {year: path} for files with the dataset's tag and a single 4-digit year.
    """
    ds = cfg["datasets"][name]
    files = {}
    for f in os.listdir(ds["dir"]):
        year_match = re.findall("[0-9]{4,4}", f)
        if len(year_match) == 1 and ds["tag"] in f:
            files[int(year_match[0])] = os.path.join(ds["dir"], f)
    return files


def address_reference_date(cfg, year):
    """
    The date (YYYYMMDD string) on which a person's address is looked up. Used both for deciding who is
    in the population of `year` (script 02) and for the location of the person (script 05).
    """
    rule = cfg["settings"]["address_reference"]
    if rule == "previous_year_dec31":
        return f"{year-1}1231"
    if rule == "year_jan1":
        return f"{year}0101"
    raise ValueError(f"Unknown settings.address_reference '{rule}' in config.json")


def read_addresses_on_reference_date(cfg, year):
    """
    Reads the latest GBAADRESOBJECTBUS csv and keeps the address registrations valid on the
    reference date of `year`.
    """
    import polars as pl
    fn = dataset_path(cfg, "gbaadresobjectbus")
    ref = address_reference_date(cfg, year)
    print(f"Reading all addresses from {fn}...")
    addresses = pl.read_csv(fn, separator=dataset_option(cfg, "gbaadresobjectbus", "sep", default=","))
    print(f"Done. Total number of records is {addresses.shape[0]}.")
    print(f"Selecting addresses registered on {ref}...")
    return addresses.filter(
        (pl.col("GBADATUMEINDEADRESHOUDING").cast(pl.String) >= ref) &
        (pl.col("GBADATUMAANVANGADRESHOUDING").cast(pl.String) <= ref)
    )
