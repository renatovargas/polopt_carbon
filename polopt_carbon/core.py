"""
core.py
--------
Main processing pipeline for computing carbon coefficients from
Land Use/Land Cover (LULC) rasters and carbon-zone polygons.

Steps:
1. Read and align inputs
2. Clip carbon zones by boundary
3. Rasterize carbon zones to LULC grid
4. Cross-tabulate LULC × Zone
5. Join zone attributes and R&G coefficients
6. Apply fallback rules (savanna/wetland)
7. Aggregate to per-LULC coefficients
8. Write outputs (QA + InVEST table)
"""

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
) -> dict:
    """Core processing routine for POLoPT Carbon."""

    logging.info(f"Starting compute() for {country}")

    # -----------------------------------------------------------------------
    # 1. Read raster and vector inputs
    # -----------------------------------------------------------------------
    logging.info("Reading input datasets…")
    lulc_arr, lulc_meta, lulc_transform, lulc_crs = read_lulc_raster(lulc)
    lulc_shape = lulc_arr.shape

    gdf_zones = read_vector(zones, target_crs=lulc_crs)
    gdf_boundary = read_vector(boundary, target_crs=lulc_crs)

    # -----------------------------------------------------------------------
    # 2. Clip carbon zones by boundary
    # -----------------------------------------------------------------------
    logging.info("Clipping carbon zones to country boundary…")
    gdf_zones_clip = gpd.clip(gdf_zones, gdf_boundary)
    logging.debug(f"Clipped features: {len(gdf_zones_clip)}")

    # -----------------------------------------------------------------------
    # 3. Rasterize carbon zones to LULC grid
    # -----------------------------------------------------------------------
    logging.info("Rasterizing carbon zones…")
    shapes = (
        (geom, val)
        for geom, val in zip(gdf_zones_clip.geometry, gdf_zones_clip.index + 1)
    )
    cz_raster = features.rasterize(
        shapes=shapes,
        out_shape=lulc_shape,
        transform=lulc_transform,
        fill=0,
        dtype="int32",
    )

    # -----------------------------------------------------------------------
    # 4. Cross-tabulate LULC × Zone
    # -----------------------------------------------------------------------
    logging.info("Building cross-tabulation…")
    mask = (cz_raster > 0) & (~np.isnan(lulc_arr))
    lulc_flat = lulc_arr[mask].astype(int)
    cz_flat = cz_raster[mask].astype(int)

    df_ct = pd.crosstab(lulc_flat, cz_flat)
    df_ct = df_ct.stack().reset_index()
    df_ct.columns = ["LULC", "CZ_ID", "Count"]
    logging.debug(f"Cross-tab entries: {len(df_ct)}")

    # -----------------------------------------------------------------------
    # 5. Join zone attributes (drop geometry)
    # -----------------------------------------------------------------------
    logging.info("Joining zone attributes…")
    attrs = gdf_zones_clip.reset_index(drop=True)
    if "geometry" in attrs.columns:
        attrs = attrs.drop(columns=["geometry"])
    attrs = attrs.reset_index().rename(columns={"index": "CZ_ID"})

    keep_cols = [
        c
        for c in attrs.columns
        if c in ("CZ_ID", "REGION", "GEZ_TERM", "FRONTIER", "CODE")
    ]
    attrs = attrs[keep_cols]

    df_out = df_ct.merge(attrs, on="CZ_ID", how="left")

    # -----------------------------------------------------------------------
    # 6. Join Ruesch & Gibbs coefficient lookup
    # -----------------------------------------------------------------------
    if coeff_lookup is not None and Path(coeff_lookup).exists():
        logging.info(f"Reading coefficient lookup: {coeff_lookup}")
        lut = read_coeff_lookup(coeff_lookup)

        # Normalize
        for col in ("REGION", "GEZ_TERM", "FRONTIER"):
            if col in lut.columns:
                lut[col] = lut[col].astype(str).str.strip()
        if "LULC" in lut.columns:
            lut["LULC"] = lut["LULC"].astype(int)

        for col in ("REGION", "GEZ_TERM", "FRONTIER"):
            if col in df_out.columns:
                df_out[col] = df_out[col].astype(str).str.strip()
        df_out["LULC"] = df_out["LULC"].astype(int)

        # Try semantic join first
        df_out_before = len(df_out)
        df_out = df_out.merge(
            lut[["REGION", "GEZ_TERM", "FRONTIER", "LULC", "carbon_value"]],
            on=["REGION", "GEZ_TERM", "FRONTIER", "LULC"],
            how="left",
        )

        # Fallback to CODE-based join if needed
        if df_out["carbon_value"].isna().all() and "CODE" in df_out.columns:
            logging.warning(
                "Semantic join produced no matches; falling back to CODE-based merge."
            )
            df_out = df_out.drop(columns=["carbon_value"], errors="ignore").merge(
                lut[["LULC", "carbon_value"]],
                how="left",
                on="LULC",
            )

        logging.info(
            f"Joined coefficient lookup for {len(df_out)} rows (was {df_out_before})."
        )

    else:
        logging.warning(
            "No coefficient lookup provided; 'carbon_value' will be missing."
        )

    # -----------------------------------------------------------------------
    # 7. Apply fallback rules (savanna/wetland, etc.)
    # -----------------------------------------------------------------------
    if "carbon_value" in df_out.columns:
        logging.info("Applying fallback rules for missing classes…")
        df_out = apply_fallback_rules(df_out)
        base_col = (
            "carbon_value_adj"
            if "carbon_value_adj" in df_out.columns
            else "carbon_value"
        )
    else:
        base_col = "carbon_value"

    # -----------------------------------------------------------------------
    # 8. Area-weighted aggregation to per-LULC coefficients
    # -----------------------------------------------------------------------
    logging.info("Aggregating to per-LULC coefficients (area-weighted)…")
    df_out["w"] = df_out["Count"].astype(float)
    weighted = (
        df_out.dropna(subset=[base_col])
        .assign(wcv=lambda d: d["w"] * d[base_col])
        .groupby("LULC", as_index=False)
        .agg(total_w=("w", "sum"), total_wcv=("wcv", "sum"))
    )
    weighted["c_above"] = weighted["total_wcv"] / weighted["total_w"]

    invest = weighted[["LULC", "c_above"]].rename(columns={"LULC": "lucode"})
    full = (
        pd.DataFrame({"lucode": list(range(1, 18))})
        .merge(invest, on="lucode", how="left")
        .fillna({"c_above": 0.0})
    )

    # -----------------------------------------------------------------------
    # 9. Write outputs
    # -----------------------------------------------------------------------
    logging.info("Writing outputs…")

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
    }
