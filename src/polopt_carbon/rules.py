"""Fallback rules for classes without a lookup value, using MODIS/IGBP codes."""

from __future__ import annotations

import logging

import pandas as pd

from polopt_carbon.config import Rules

logger = logging.getLogger(__name__)

FOREST_CODES = [1, 2, 3, 4]
SHRUB_CODES = [6, 7]
SAVANNA_CODES = [8, 9]
WETLAND_CODE = 11


def apply_fallback_rules(df: pd.DataFrame, rules: Rules | None = None) -> pd.DataFrame:
    """Fill missing values, adding ``CARBON_VALUE_ADJ`` and ``rule_applied`` columns.

    The reference values are unweighted means over the rows in ``df``, so they
    depend on the country being processed.
    """
    rules = rules or Rules()
    df = df.copy()
    df["LULC"] = pd.to_numeric(df["LULC"], errors="coerce")
    df["CARBON_VALUE_ADJ"] = df["CARBON_VALUE"]
    df["rule_applied"] = None

    forest_ref = df.loc[df["LULC"].isin(FOREST_CODES), "CARBON_VALUE"].mean()
    shrub_ref = df.loc[df["LULC"].isin(SHRUB_CODES), "CARBON_VALUE"].mean()

    if pd.notna(forest_ref):
        fraction = rules.savanna_percent_of_forest
        mask = df["LULC"].isin(SAVANNA_CODES) & df["CARBON_VALUE_ADJ"].isna()
        if mask.any():
            df.loc[mask, "CARBON_VALUE_ADJ"] = forest_ref * fraction
            df.loc[mask, "rule_applied"] = f"savanna={fraction * 100:.0f}%_forest"
            logger.info("Applied savanna fallback (%.0f%% of forest).", fraction * 100)

    if rules.wetland_woody_equals_forest and pd.notna(forest_ref):
        mask = (df["LULC"] == WETLAND_CODE) & df["CARBON_VALUE_ADJ"].isna()
        if mask.any():
            df.loc[mask, "CARBON_VALUE_ADJ"] = forest_ref
            df.loc[mask, "rule_applied"] = "wetland_woody=forest"
            logger.info("Applied wetland_woody=forest fallback.")

    if rules.marsh_equals_shrub and pd.notna(shrub_ref):
        mask = (df["LULC"] == WETLAND_CODE) & df["CARBON_VALUE_ADJ"].isna()
        if mask.any():
            df.loc[mask, "CARBON_VALUE_ADJ"] = shrub_ref
            df.loc[mask, "rule_applied"] = "wetland_to_shrub"
            logger.info("Applied wetland_to_shrub fallback.")

    df["rule_applied"] = df["rule_applied"].fillna("none")
    return df
