"""
core.py
--------
Main processing pipeline for computing carbon coefficients and mapping.
"""

from pathlib import Path
import logging
import geopandas as gpd
import pandas as pd
import numpy as np
import rasterio
from rasterio import features
from importlib import resources

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
    crosswalk_path: Path | None = None,
    invest_table_out: Path | None = None,
    method: str = "dominant",
    expert_rules: Path | None = None,
    force_wetland_overrides: bool = False,
) -> dict:
    """Core processing routine for POLoPT Carbon."""

    logging.info(f"Starting compute() for {country} [method={method}]")

    # 1. Resolve Bundled Data Paths
    if crosswalk_path is None:
        traversable = resources.files("polopt_carbon.data").joinpath(
            "modis_glc_crosswalk.csv"
        )
        with resources.as_file(traversable) as p:
            crosswalk_path = p
    else:
        crosswalk_path = Path(crosswalk_path).expanduser()

    if coeff_lookup is None:
        traversable = resources.files("polopt_carbon.data").joinpath(
            "rg_lookup_exploded.csv"
        )
        with resources.as_file(traversable) as p:
            coeff_lookup = p
    else:
        coeff_lookup = Path(coeff_lookup).expanduser()

    # 2. Read raster and vector inputs
    lulc_arr, lulc_meta, lulc_transform, lulc_crs = read_lulc_raster(lulc)
    lulc_shape = lulc_arr.shape

    gdf_zones = read_vector(zones, target_crs=lulc_crs)
    gdf_boundary = read_vector(boundary, target_crs=lulc_crs)

    # Calculate pixel area in hectares
    pixel_area_ha = abs(lulc_transform.a * lulc_transform.e) / 10_000.0
    logging.info(f"Pixel area (ha): {pixel_area_ha:.6f}")

    # 3. Clip and Rasterize Carbon Zones
    gdf_zones_clip = gpd.clip(gdf_zones, gdf_boundary).reset_index(drop=True)

    shapes = ((geom, idx + 1) for idx, geom in enumerate(gdf_zones_clip.geometry))
    cz_raster = features.rasterize(
        shapes=shapes,
        out_shape=lulc_shape,
        transform=lulc_transform,
        fill=0,
        dtype="int32",
    )

    # 4. Cross-tabulate LULC x Zone
    mask = (cz_raster > 0) & (~np.isnan(lulc_arr))
    df_out = pd.DataFrame(
        {"LULC": lulc_arr[mask].astype(int), "CZ_ID": cz_raster[mask].astype(int)}
    )
    df_out = df_out.groupby(["LULC", "CZ_ID"]).size().reset_index(name="Count")

    # 5. Join Attributes and Crosswalk
    attrs = gdf_zones_clip.drop(columns=["geometry"], errors="ignore").reset_index()
    attrs = attrs.rename(columns={"index": "CZ_ID"})
    attrs["CZ_ID"] += 1
    attrs = attrs[["CZ_ID", "REGION", "GEZ_TERM", "FRONTIER"]]

    df_out = df_out.merge(attrs, on="CZ_ID", how="left")

    crosswalk = pd.read_csv(crosswalk_path)
    df_out = df_out.merge(
        crosswalk[["LULC", "LULC_CLASS", "GLC2000 Class"]], on="LULC", how="left"
    )
    df_out = df_out.rename(columns={"GLC2000 Class": "GLC2000"})

    # 6. Join R&G Coefficients and Apply Rules
    lut = read_coeff_lookup(coeff_lookup)
    df_out = df_out.merge(
        lut, on=["GLC2000", "REGION", "GEZ_TERM", "FRONTIER"], how="left"
    )

    # --- FIX: Pack parameters into the 'config' dictionary your rules.py expects ---
    rules_cfg = {
        "savanna_percent_of_forest": 0.4,
        "wetland_woody_equals_forest": True,
        "marsh_equals_shrub": True,
        "force_wetland_overrides": force_wetland_overrides,
    }
    df_out = apply_fallback_rules(df_out, config=rules_cfg)

    # Identify the correct column after rules are applied
    base_col = (
        "CARBON_VALUE_ADJ" if "CARBON_VALUE_ADJ" in df_out.columns else "CARBON_VALUE"
    )

    # 7. Calculate Total Carbon (Mass)
    df_out["TOTAL_CARBON"] = df_out["Count"] * df_out[base_col] * pixel_area_ha

    # 8. Generate Carbon Density Raster (.tif)
    carbon_raster_path = out.parent / f"{country.lower()}_carbon_density.tif"
    carbon_map_dict = df_out.set_index(["LULC", "CZ_ID"])[base_col].to_dict()
    carbon_arr = np.zeros(lulc_shape, dtype="float32")
    for (l_val, cz_val), c_val in carbon_map_dict.items():
        pixel_mask = (lulc_arr == l_val) & (cz_raster == cz_val)
        carbon_arr[pixel_mask] = c_val

    new_meta = lulc_meta.copy()
    new_meta.update(dtype="float32", count=1, nodata=0)
    with rasterio.open(carbon_raster_path, "w", **new_meta) as dst:
        dst.write(carbon_arr, 1)

    # 9. Aggregate for InVEST
    if method.lower() == "dominant":
        idx = df_out.groupby("LULC")["Count"].idxmax()
        invest = df_out.loc[idx, ["LULC", base_col]].rename(
            columns={base_col: "c_above", "LULC": "lucode"}
        )
    else:
        weighted = (
            df_out.dropna(subset=[base_col])
            .assign(wcv=lambda d: d["Count"] * d[base_col])
            .groupby("LULC")
            .agg(total_w=("Count", "sum"), total_wcv=("wcv", "sum"))
        )
        weighted["c_above"] = weighted["total_wcv"] / weighted["total_w"]
        invest = weighted.reset_index()[["LULC", "c_above"]].rename(
            columns={"LULC": "lucode"}
        )

    # Apply Expert Overrides to InVEST table
    full = (
        pd.DataFrame({"lucode": range(1, 18)})
        .merge(invest, on="lucode", how="left")
        .fillna(0.0)
    )
    if expert_rules and Path(expert_rules).exists():
        expert_df = pd.read_csv(expert_rules)
        full = full.merge(expert_df, on="lucode", how="left")
        if "c_above_override" in full.columns:
            full["c_above"] = full["c_above_override"].combine_first(full["c_above"])

    # 10. Final Output Writing
    cols = [
        "LULC",
        "LULC_CLASS",
        "CZ_ID",
        "REGION",
        "GEZ_TERM",
        "Count",
        base_col,
        "TOTAL_CARBON",
    ]
    # Filter to only existing columns to avoid key errors
    existing_cols = [c for c in cols if c in df_out.columns]
    other_cols = [c for c in df_out.columns if c not in existing_cols]
    df_out = df_out[existing_cols + other_cols]

    write_dataframe(df_out, out)
    if invest_table_out:
        write_dataframe(full, invest_table_out)
    if out_gpkg:
        write_geopackage({"carbon_zones_clipped": gdf_zones_clip}, out_gpkg)

    logging.info(f"Run complete. Table: {out}")
    return {"country": country, "raster": str(carbon_raster_path), "table": str(out)}
