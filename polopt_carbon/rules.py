"""
rules.py — fallback rules using numeric MODIS/IGBP codes (1–17).
Applies simple, global fallbacks for missing values only.
(Advanced wetland overrides are handled in core.py)
"""

from __future__ import annotations
import logging
import pandas as pd


def apply_fallback_rules(df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
    """
    Apply generic fallback rules for missing CARBON_VALUEs.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing at least 'LULC' and 'CARBON_VALUE'.
    config : dict, optional
        Dictionary of configuration flags:
          - savanna_percent_of_forest (float)
          - wetland_woody_equals_forest (bool)
          - marsh_equals_shrub (bool)

    Returns
    -------
    pd.DataFrame
        Same DataFrame with 'CARBON_VALUE_ADJ' and 'rule_applied' columns.
    """
    if config is None:
        config = {
            "savanna_percent_of_forest": 0.4,
            "wetland_woody_equals_forest": True,
            "marsh_equals_shrub": True,
        }

    df = df.copy()
    df["LULC"] = pd.to_numeric(df["LULC"], errors="coerce")

    if "CARBON_VALUE" not in df.columns:
        raise KeyError("Expected column 'CARBON_VALUE' not found in dataframe.")

    # Prepare columns
    df["CARBON_VALUE_ADJ"] = df["CARBON_VALUE"]
    df["rule_applied"] = None

    # -----------------------------------------------------------------------
    # Reference values for simple rules
    # -----------------------------------------------------------------------
    forest_ref = df.loc[df["LULC"].isin([1, 2, 3, 4]), "CARBON_VALUE"].mean()
    shrub_ref = df.loc[df["LULC"].isin([6, 7]), "CARBON_VALUE"].mean()

    # -----------------------------------------------------------------------
    # 1) Savannas (8–9) = fraction of forest, if missing
    # -----------------------------------------------------------------------
    if pd.notna(forest_ref) and "savanna_percent_of_forest" in config:
        frac = config["savanna_percent_of_forest"]
        sav_mask = df["LULC"].isin([8, 9]) & df["CARBON_VALUE_ADJ"].isna()
        if sav_mask.any():
            df.loc[sav_mask, "CARBON_VALUE_ADJ"] = forest_ref * frac
            df.loc[sav_mask, "rule_applied"] = f"savanna={frac * 100:.0f}%_forest"
            logging.info(f"Applied savanna fallback ({frac * 100:.0f}% of forest).")

    # -----------------------------------------------------------------------
    # 2) Woody wetlands (11) = forest value if missing
    # -----------------------------------------------------------------------
    if config.get("wetland_woody_equals_forest", False) and pd.notna(forest_ref):
        wet_mask = (df["LULC"] == 11) & df["CARBON_VALUE_ADJ"].isna()
        if wet_mask.any():
            df.loc[wet_mask, "CARBON_VALUE_ADJ"] = forest_ref
            df.loc[wet_mask, "rule_applied"] = "wetland_woody=forest"
            logging.info("Applied wetland_woody=forest fallback.")

    # -----------------------------------------------------------------------
    # 3) Marsh (11) = shrub value if missing and no forest rule applied
    # -----------------------------------------------------------------------
    if config.get("marsh_equals_shrub", False) and pd.notna(shrub_ref):
        marsh_mask = (df["LULC"] == 11) & df["CARBON_VALUE_ADJ"].isna()
        if marsh_mask.any():
            df.loc[marsh_mask, "CARBON_VALUE_ADJ"] = shrub_ref
            df.loc[marsh_mask, "rule_applied"] = "wetland_to_shrub"
            logging.info("Applied wetland_to_shrub fallback.")

    # -----------------------------------------------------------------------
    # Finalize
    # -----------------------------------------------------------------------
    df["rule_applied"] = df["rule_applied"].fillna("none")
    return df
