# core.py
# --------
from pathlib import Path
import logging
import geopandas as gpd
import pandas as pd
import numpy as np
from rasterio import features

from polopt_carbon.io import (
    read_lulc_raster,
    read_vector,
    write_dataframe,
    write_geopackage,
    write_run_metadata,
    read_coeff_lookup,
)
from polopt_carbon.rules import apply_fallback_rules


def compute(
    country: str,
    lulc: Path,
    zones: Path,
    boundary: Path,
    out: Path,
    out_gpkg: Path | None = None,
    overwrite: bool = False,
    coeff_lookup: Path | None = None,
    invest_table_out: Path | None = None,
    method: str = "dominant",
    expert_rules: Path | None = None,
    force_wetland_overrides: bool = False,
) -> dict:
    """Core processing routine for POLoPT Carbon."""

    logging.info(f"Starting compute() for {country} [method={method}]")

    # -----------------------------------------------------------------------
    # 1. Read raster and vector inputs
    # -----------------------------------------------------------------------
    logging.info("Reading input datasets…")
    lulc_arr, lulc_meta, lulc_transform, lulc_crs = read_lulc_raster(
        lulc
    )  # Read LULC raster

    lulc_shape = lulc_arr.shape

    gdf_zones = read_vector(zones, target_crs=lulc_crs)
    gdf_boundary = read_vector(boundary, target_crs=lulc_crs)

    # Compute pixel area in hectares
    pixel_width = lulc_transform.a
    pixel_height = lulc_transform.e
    pixel_area_ha = abs(pixel_width * pixel_height) / 10_000.0
    logging.info(f"Pixel area (ha): {pixel_area_ha:.6f}")

    # -----------------------------------------------------------------------
    # 2. Clip carbon zones by boundary
    # -----------------------------------------------------------------------
    logging.info("Clipping carbon zones to country boundary…")
    gdf_zones_clip = gpd.clip(gdf_zones, gdf_boundary).reset_index(
        drop=True
    )  # Clip GeoDataFrame

    # -----------------------------------------------------------------------
    # 3. Rasterize carbon zones to LULC grid
    # -----------------------------------------------------------------------
    logging.info("Rasterizing carbon zones (feature index as ID)…")
    shapes = ((geom, idx + 1) for idx, geom in enumerate(gdf_zones_clip.geometry))
    cz_raster = features.rasterize(  # Rasterize vector features
        shapes=shapes,
        out_shape=lulc_shape,
        transform=lulc_transform,
        fill=0,
        dtype="int32",
    )

    # -----------------------------------------------------------------------
    # 4. Cross-tabulate LULC × Zone
    # -----------------------------------------------------------------------
    mask = (cz_raster > 0) & (~np.isnan(lulc_arr))
    lulc_flat = lulc_arr[mask].astype(int)
    cz_flat = cz_raster[mask].astype(int)

    df_ct = (
        pd.crosstab(lulc_flat, cz_flat).stack().reset_index()
    )  # Cross-tabulate LULC/Zone
    df_ct.columns = ["LULC", "CZ_ID", "Count"]

    # -----------------------------------------------------------------------
    # 5. Join zone attributes
    # -----------------------------------------------------------------------
    attrs = gdf_zones_clip.drop(columns=["geometry"], errors="ignore").copy()
    attrs = attrs.reset_index().rename(columns={"index": "CZ_ID"})
    attrs["CZ_ID"] = attrs["CZ_ID"] + 1
    attrs = attrs[["CZ_ID", "REGION", "GEZ_TERM", "FRONTIER"]]
    df_out = df_ct.merge(attrs, on="CZ_ID", how="left")  # Merge zone attributes

    # -----------------------------------------------------------------------
    # 6. Read MODIS–GLC2000 crosswalk and attach GLC2000 class
    # -----------------------------------------------------------------------
    crosswalk_path = Path("~/testdata/uga/lookup/modis_glc_crosswalk.csv").expanduser()
    if crosswalk_path.exists():
        crosswalk = pd.read_csv(crosswalk_path)
        df_out = df_out.merge(  # Merge crosswalk data
            crosswalk[["LULC", "LULC_CLASS", "GLC2000 Class"]],
            on="LULC",
            how="left",
        )
        df_out = df_out.rename(columns={"GLC2000 Class": "GLC2000"})
    else:
        logging.warning(f"Crosswalk not found: {crosswalk_path}")
        df_out["LULC_CLASS"] = None
        df_out["GLC2000"] = None

    # -----------------------------------------------------------------------
    # 7. Join Ruesch & Gibbs coefficients by GLC2000 + REGION + GEZ_TERM + FRONTIER
    # -----------------------------------------------------------------------
    if coeff_lookup is not None and Path(coeff_lookup).exists():
        lut = read_coeff_lookup(coeff_lookup)

        for col in ("REGION", "GEZ_TERM", "FRONTIER", "GLC2000"):
            if col in lut.columns:
                lut[col] = lut[col].astype(str).str.strip()
        for col in ("REGION", "GEZ_TERM", "FRONTIER", "GLC2000"):
            if col in df_out.columns:
                df_out[col] = df_out[col].astype(str).str.strip()

        df_out = df_out.merge(  # Merge carbon coefficients
            lut[["GLC2000", "REGION", "GEZ_TERM", "FRONTIER", "CARBON_VALUE"]],
            on=["GLC2000", "REGION", "GEZ_TERM", "FRONTIER"],
            how="left",
        )

        # Global fallback
        global_mask = df_out["CARBON_VALUE"].isna() & df_out["GLC2000"].isin(
            [
                "16: Cultivated and managed land",
                "19: Bare areas",
                "20 - 23: Water, snow and ice;artificial surfaces",
            ]
        )
        if global_mask.any():
            global_lut = lut[lut["REGION"].str.lower() == "global"]
            df_out.loc[global_mask, "CARBON_VALUE"] = df_out.loc[
                global_mask, "GLC2000"
            ].map(
                global_lut.set_index("GLC2000")["CARBON_VALUE"]
            )  # Map global fallbacks
    else:
        logging.warning(
            "No coefficient lookup provided; 'CARBON_VALUE' will be missing."
        )

    # -----------------------------------------------------------------------
    # 8. Fix missing GLC2000 and zero-carbon conditions
    # -----------------------------------------------------------------------
    df_out["GLC2000"] = df_out["GLC2000"].fillna(df_out["LULC_CLASS"])

    # Ensure Woody Savannas (8) and Savannas (9) copy LULC_CLASS for readability
    mask_savanna = df_out["LULC"].isin([8, 9])
    df_out.loc[mask_savanna, "GLC2000"] = df_out.loc[mask_savanna, "LULC_CLASS"]

    # Force CARBON_VALUE = 0 only for Water GEZ or Urban/Water LULC
    zero_mask = (df_out["GEZ_TERM"] == "Water") | (df_out["LULC"].isin([13, 17]))
    df_out.loc[zero_mask, "CARBON_VALUE"] = 0

    # Leave 8 & 9 as NaN so fallback_rules() applies the 40% forest rule

    # -----------------------------------------------------------------------
    # 9. Apply fallback rules (savanna/wetland, etc.)
    # -----------------------------------------------------------------------
    rules_cfg = {
        "savanna_percent_of_forest": 0.4,
        "wetland_woody_equals_forest": True,
        "marsh_equals_shrub": True,
        "force_wetland_overrides": force_wetland_overrides,
    }

    if "CARBON_VALUE" in df_out.columns:
        df_out = apply_fallback_rules(df_out, config=rules_cfg)  # Apply custom rules
        base_col = (
            "CARBON_VALUE_ADJ"
            if "CARBON_VALUE_ADJ" in df_out.columns
            else "CARBON_VALUE"
        )
    else:
        base_col = "CARBON_VALUE"

    # -----------------------------------------------------------------------
    # 10. Export overlay table
    # -----------------------------------------------------------------------
    if invest_table_out is not None:
        overlay_out = invest_table_out.parent / f"{country.lower()}_overlay_counts.csv"
        logging.info("Writing overlay table…")

        group_cols = ["LULC", "LULC_CLASS", "GLC2000", "REGION", "GEZ_TERM", "FRONTIER"]
        df_overlay = (
            df_out[group_cols + ["Count", base_col]]
            .groupby(group_cols, as_index=False, dropna=False)
            .agg(Count=("Count", "sum"))
        )
        df_overlay = df_overlay.merge(
            df_out[group_cols + [base_col]].drop_duplicates(), on=group_cols, how="left"
        )
        df_overlay = df_overlay.rename(columns={base_col: "CARBON_VALUE"})
        df_overlay["TOTAL_CARBON"] = (
            df_overlay["Count"] * pixel_area_ha * df_overlay["CARBON_VALUE"]
        )
        df_overlay = df_overlay[
            [
                "LULC",
                "LULC_CLASS",
                "GLC2000",
                "REGION",
                "GEZ_TERM",
                "FRONTIER",
                "Count",
                "CARBON_VALUE",
                "TOTAL_CARBON",
            ]
        ].sort_values(["LULC", "REGION", "GEZ_TERM", "FRONTIER"])
        write_dataframe(df_overlay, overlay_out)

    # -----------------------------------------------------------------------
    # 11. Aggregate to per-LULC coefficients (for InVEST)
    # -----------------------------------------------------------------------
    df_out["w"] = df_out["Count"].astype(float)

    if method.lower() == "dominant":
        logging.info("Selecting dominant GEZ/region/frontier per LULC (most pixels)…")
        idx = df_out.groupby("LULC")["w"].idxmax()  # Find index max
        invest = df_out.loc[idx, ["LULC", base_col]].rename(
            columns={base_col: "c_above", "LULC": "lucode"}
        )

    elif method.lower() == "weighted":
        logging.info("Computing weighted-average coefficients per LULC…")
        weighted = (
            df_out.dropna(subset=[base_col])
            .assign(wcv=lambda d: d["w"] * d[base_col])
            .groupby("LULC", as_index=False)
            .agg(total_w=("w", "sum"), total_wcv=("wcv", "sum"))
        )
        weighted["c_above"] = (
            weighted["total_wcv"] / weighted["total_w"]
        )  # Calculate weighted-average
        invest = weighted[["LULC", "c_above"]].rename(columns={"LULC": "lucode"})

    else:
        raise ValueError("Invalid method. Use 'dominant' or 'weighted'.")

    full = (
        pd.DataFrame({"lucode": list(range(1, 18))})
        .merge(invest, on="lucode", how="left")
        .fillna({"c_above": 0.0})
    )

    # -----------------------------------------------------------------------
    # 12. Apply expert overrides (if provided)
    # -----------------------------------------------------------------------
    if expert_rules and Path(expert_rules).exists():
        logging.info(f"Applying expert overrides from {expert_rules}…")
        expert_df = pd.read_csv(expert_rules)
        if {"lucode", "c_above_override"}.issubset(expert_df.columns):
            full = full.merge(expert_df, on="lucode", how="left", suffixes=("", "_exp"))
            full["c_above"] = full["c_above_override"].combine_first(
                full["c_above"]
            )  # Apply expert overrides
            full.drop(columns=["c_above_override"], inplace=True)
        else:
            logging.warning(
                "Expert file missing required columns ('lucode', 'c_above_override')."
            )

    # -----------------------------------------------------------------------
    # 13. Write outputs
    # -----------------------------------------------------------------------
    if out_gpkg:
        write_geopackage({"carbon_zones_clipped": gdf_zones_clip}, out_gpkg)
    if invest_table_out is not None:
        invest_table_out.parent.mkdir(parents=True, exist_ok=True)
        write_dataframe(full, invest_table_out)

    write_run_metadata(
        {
            "country": country,
            "lulc": str(lulc),
            "zones": str(zones),
            "boundary": str(boundary),
            "records_ct": len(df_out),
            "invest_rows": len(full),
            "method": method,
            "expert_rules": str(expert_rules) if expert_rules else None,
            "force_wetland_overrides": force_wetland_overrides,
        },
        out.parent,
    )

    logging.info("Processing complete.")
    return {
        "country": country,
        "records_ct": len(df_out),
        "out": str(out),
        "out_gpkg": str(out_gpkg) if out_gpkg else None,
        "invest_table": str(invest_table_out) if invest_table_out else None,
        "method": method,
        "expert_rules": str(expert_rules) if expert_rules else None,
        "force_wetland_overrides": force_wetland_overrides,
    }
