"""Coefficient tables and how pixels are matched to them."""

from __future__ import annotations

import difflib
from importlib import resources
from pathlib import Path

import pandas as pd

from polopt_carbon.config import InputError, Rules
from polopt_carbon.rules import SAVANNA_CODES, WETLAND_CODE, apply_fallback_rules

JOIN_KEYS = ["GLC2000", "REGION", "GEZ_TERM", "FRONTIER"]
GLOBAL_LABEL = "Global"
# The crosswalk and lookup spell this class differently, and one lookup label
# has a typo. Both are only reconciled when use_global_classes is on.
GLC_LABEL_ALIASES = {
    "20 - 23: Water, snow and ice, and artificial surfaces": (
        "20 - 23: Water, snow and ice;artificial surfaces"
    ),
}
GEZ_LABEL_FIXES = {"Temperate oceanic forests": "Temperate oceanic forest"}

SOURCE_LOOKUP = "lookup"
SOURCE_GLOBAL = "global"
SOURCE_RULE = "rule"
SOURCE_UNMATCHED = "unmatched"


def _data_file(name: str):
    return resources.files("polopt_carbon.data").joinpath(name)


def _read_table(source, label: str, required: set[str]) -> pd.DataFrame:
    try:
        table = pd.read_csv(source)
    except (OSError, ValueError) as exc:
        raise InputError(f"Cannot read {label} table {source}: {exc}") from exc
    missing = required - set(table.columns)
    if missing:
        raise InputError(
            f"{label} table {source} is missing columns: {', '.join(sorted(missing))}"
        )
    return table


def load_crosswalk(path: Path | None = None) -> pd.DataFrame:
    source = path or _data_file("modis_glc_crosswalk.csv")
    required = {"LULC", "LULC_CLASS", "GLC2000 Class"}
    table = _read_table(source, "Crosswalk", required)
    return table[["LULC", "LULC_CLASS", "GLC2000 Class"]].rename(
        columns={"GLC2000 Class": "GLC2000"}
    )


def load_lookup(path: Path | None = None) -> pd.DataFrame:
    source = path or _data_file("rg_lookup_exploded.csv")
    return _read_table(source, "Lookup", {*JOIN_KEYS, "CARBON_VALUE"})


def global_values(lookup: pd.DataFrame) -> dict[str, float]:
    """Return values from the lookup's Global rows, by crosswalk or lookup label."""
    rows = lookup[lookup["REGION"] == GLOBAL_LABEL]
    values = dict(zip(rows["GLC2000"], rows["CARBON_VALUE"], strict=True))
    for crosswalk_label, lookup_label in GLC_LABEL_ALIASES.items():
        if lookup_label in values:
            values[crosswalk_label] = values[lookup_label]
    return values


def attach_coefficients(
    df: pd.DataFrame,
    crosswalk: pd.DataFrame,
    lookup: pd.DataFrame,
    rules: Rules | None = None,
    use_global_classes: bool = False,
) -> pd.DataFrame:
    """Add class names, lookup values, rule results and ``VALUE_SOURCE``."""
    df = df.merge(crosswalk, on="LULC", how="left")
    table = lookup
    if use_global_classes:
        table = lookup.assign(GEZ_TERM=lookup["GEZ_TERM"].replace(GEZ_LABEL_FIXES))
    df = df.merge(table, on=JOIN_KEYS, how="left")

    from_global = pd.Series(False, index=df.index)
    if use_global_classes:
        fallback = df["GLC2000"].map(global_values(lookup))
        from_global = df["CARBON_VALUE"].isna() & fallback.notna()
        df.loc[from_global, "CARBON_VALUE"] = fallback[from_global]

    df = apply_fallback_rules(df, rules)
    df["VALUE_SOURCE"] = SOURCE_UNMATCHED
    df.loc[df["CARBON_VALUE"].notna(), "VALUE_SOURCE"] = SOURCE_LOOKUP
    df.loc[from_global, "VALUE_SOURCE"] = SOURCE_GLOBAL
    df.loc[df["rule_applied"] != "none", "VALUE_SOURCE"] = SOURCE_RULE
    df["CARBON_VALUE_ADJ"] = df["CARBON_VALUE_ADJ"].fillna(0.0)
    return df


def zone_options(lookup: pd.DataFrame) -> tuple[list[str], list[str]]:
    usable = lookup[lookup["REGION"] != GLOBAL_LABEL]
    return sorted(usable["REGION"].unique()), sorted(usable["GEZ_TERM"].unique())


def _resolve_name(value: str, options: list[str], label: str) -> str:
    matches = {option.lower(): option for option in options}
    if value.lower() in matches:
        return matches[value.lower()]
    close = difflib.get_close_matches(value, options, n=3, cutoff=0.5)
    hint = f" Did you mean: {', '.join(close)}?" if close else ""
    raise InputError(f"Unknown {label} '{value}'.{hint}")


def coefficients_for_zone(
    region: str,
    gez_term: str,
    frontier: int = 0,
    use_global_classes: bool = False,
    crosswalk_path: Path | None = None,
    lookup_path: Path | None = None,
    rules: Rules | None = None,
) -> pd.DataFrame:
    """Show what each land cover class receives in one carbon zone."""
    rules = rules or Rules()
    crosswalk = load_crosswalk(crosswalk_path)
    lookup = load_lookup(lookup_path)
    regions, terms = zone_options(lookup)
    region = _resolve_name(region, regions, "region")
    gez_term = _resolve_name(gez_term, terms, "GEZ term")

    df = crosswalk[["LULC"]].assign(REGION=region, GEZ_TERM=gez_term, FRONTIER=frontier)
    df = attach_coefficients(df, crosswalk, lookup, rules, use_global_classes)
    # Rule values depend on the country's forest rows, so describe them instead.
    fraction = rules.savanna_percent_of_forest
    notes = {
        SOURCE_LOOKUP: "From the lookup table",
        SOURCE_GLOBAL: "From the lookup's Global row",
    }
    df["NOTE"] = df["VALUE_SOURCE"].map(notes)
    missing = df["CARBON_VALUE"].isna()
    savanna = missing & df["LULC"].isin(SAVANNA_CODES)
    wetland = missing & (df["LULC"] == WETLAND_CODE)
    wetland &= rules.wetland_woody_equals_forest
    df.loc[savanna, "VALUE_SOURCE"] = SOURCE_RULE
    df.loc[wetland, "VALUE_SOURCE"] = SOURCE_RULE
    df.loc[savanna, "NOTE"] = (
        f"Rule: {fraction * 100:.0f}% of the country's mean forest value"
    )
    df.loc[wetland, "NOTE"] = "Rule: the country's mean forest value"
    unmatched = df["VALUE_SOURCE"] == SOURCE_UNMATCHED
    available = global_values(lookup)
    labels = df.loc[unmatched, "GLC2000"]
    df.loc[unmatched, "NOTE"] = [
        f"No match, so 0 (Global row has {available[label]:g})"
        if available.get(label)
        else "No match, so 0"
        for label in labels
    ]
    df["VALUE"] = df["CARBON_VALUE"]
    df.loc[unmatched & missing, "VALUE"] = 0.0
    return df[["LULC", "LULC_CLASS", "VALUE", "VALUE_SOURCE", "NOTE"]]
