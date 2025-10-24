"""
rules.py — fallback rules using numeric MODIS/IGBP codes (1–17).
Only fill when carbon_value is missing; never overwrite real values.
"""

from __future__ import annotations
import logging
import pandas as pd


def apply_fallback_rules(df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
    if config is None:
        config = {
            "savanna_percent_of_forest": 0.4,
            "wetland_woody_equals_forest": True,
            "marsh_equals_shrub": True,
        }

    df = df.copy()
    df["LULC"] = pd.to_numeric(df["LULC"], errors="coerce")
    df["carbon_value_adj"] = df["carbon_value"]
    df["rule_applied"] = None

    # Forest reference (mean of 1–4) — computed once
    forest_mean = df[df["LULC"].isin([1, 2, 3, 4])]["carbon_value"].mean()
    shrub_mean = df[df["LULC"].isin([6, 7])]["carbon_value"].mean()

    # 1) Savannas (8–9) = fraction of forest, but ONLY if missing
    if "savanna_percent_of_forest" in config and pd.notna(forest_mean):
        frac = config["savanna_percent_of_forest"]
        logging.info(
            f"Applying savanna fallback ({int(frac * 100)}% of forest) to missing values"
        )
        sav_mask = df["LULC"].isin([8, 9]) & df["carbon_value_adj"].isna()
        df.loc[sav_mask, "carbon_value_adj"] = forest_mean * frac
        df.loc[sav_mask, "rule_applied"] = f"savanna={frac * 100:.0f}%_forest"

    # 2) Woody wetlands (11) = forest, ONLY if missing
    if config.get("wetland_woody_equals_forest", False) and pd.notna(forest_mean):
        wet_mask = (df["LULC"] == 11) & df["carbon_value_adj"].isna()
        df.loc[wet_mask, "carbon_value_adj"] = forest_mean
        df.loc[wet_mask, "rule_applied"] = "wetland_woody=forest"

    # 3) Marsh = shrub (11) — optional alternative, ONLY if missing
    if config.get("marsh_equals_shrub", False) and pd.notna(shrub_mean):
        marsh_mask = (df["LULC"] == 11) & df["carbon_value_adj"].isna()
        df.loc[marsh_mask, "carbon_value_adj"] = shrub_mean
        df.loc[marsh_mask, "rule_applied"] = "wetland_to_shrub"

    df["rule_applied"].fillna("none", inplace=True)
    return df
