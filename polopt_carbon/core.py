"""
core.py
--------
Main processing pipeline for computing carbon coefficients and mapping.
Standardized to EPSG:6933 (Equal Area) via pre-reprojection.
Units: Carbon Density (Mg C / ha), Total Carbon (Mg C).
"""

from pathlib import Path
import logging
import geopandas as gpd
import pandas as pd
import numpy as np
import rasterio
from rasterio import features
import rioxarray
from importlib import resources

from polopt_carbon.io import (
    read_lulc_raster,
    read_vector,
    write_dataframe,
    write_geopackage,
    read_coeff_lookup,
)
from polopt_carbon.rules import apply_fallback_rules

# The industry standard for equal-area global carbon mapping
TARGET_CRS = "EPSG:6933"


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
    logging.info(f"Starting compute() for {country} standardized to {TARGET_CRS}")

    # 1. PRE-REPROJECTION
    gdf_zones = gpd.read_file(zones).to_crs(TARGET_CRS)
    gdf_boundary = gpd.read_file(boundary).to_crs(TARGET_CRS)

    rds = rioxarray.open_rasterio(lulc)
    rds_proj = rds.rio.reproject(TARGET_CRS)

    lulc_arr = rds_proj.values[0]
    lulc_transform = rds_proj.rio.transform()

    # Calculate Pixel Area in Hectares
    pixel_area_ha = abs(lulc_transform.a * lulc_transform.e) / 10000.0
    logging.info(f"Projected Pixel Area: {pixel_area_ha:.6f} hectares")

    # 2. SPATIAL LOGIC
    gdf_zones_clip = gpd.clip(gdf_zones, gdf_boundary).reset_index(drop=True)

    shapes = ((geom, idx + 1) for idx, geom in enumerate(gdf_zones_clip.geometry))
    cz_raster = features.rasterize(
        shapes=shapes,
        out_shape=lulc_arr.shape,
        transform=lulc_transform,
        fill=0,
        dtype="int32",
    )

    # 3. DATA EXTRACTION
    mask = (cz_raster > 0) & (~np.isnan(lulc_arr))
    df_out = pd.DataFrame(
        {"LULC": lulc_arr[mask].astype(int), "CZ_ID": cz_raster[mask].astype(int)}
    )
    df_out = df_out.groupby(["LULC", "CZ_ID"]).size().reset_index(name="Count")

    # 4. JOINS & ATTRIBUTES
    attrs = gdf_zones_clip.drop(columns=["geometry"], errors="ignore").reset_index()
    attrs = attrs.rename(columns={"index": "CZ_ID"})
    attrs["CZ_ID"] += 1
    attrs = attrs[["CZ_ID", "REGION", "GEZ_TERM", "FRONTIER"]]
    df_out = df_out.merge(attrs, on="CZ_ID", how="left")

    if crosswalk_path is None:
        with resources.as_file(
            resources.files("polopt_carbon.data").joinpath("modis_glc_crosswalk.csv")
        ) as p:
            crosswalk = pd.read_csv(p)
    else:
        crosswalk = pd.read_csv(crosswalk_path)

    df_out = df_out.merge(
        crosswalk[["LULC", "LULC_CLASS", "GLC2000 Class"]], on="LULC", how="left"
    )
    df_out = df_out.rename(columns={"GLC2000 Class": "GLC2000"})

    if coeff_lookup is None:
        with resources.as_file(
            resources.files("polopt_carbon.data").joinpath("rg_lookup_exploded.csv")
        ) as p:
            lut = pd.read_csv(p)
    else:
        lut = pd.read_csv(coeff_lookup)

    df_out = df_out.merge(
        lut, on=["GLC2000", "REGION", "GEZ_TERM", "FRONTIER"], how="left"
    )

    rules_cfg = {
        "savanna_percent_of_forest": 0.4,
        "wetland_woody_equals_forest": True,
        "marsh_equals_shrub": True,
        "force_wetland_overrides": force_wetland_overrides,
    }
    df_out = apply_fallback_rules(df_out, config=rules_cfg)

    # 5. FINAL UNITS CALCULATION
    base_col = (
        "CARBON_VALUE_ADJ" if "CARBON_VALUE_ADJ" in df_out.columns else "CARBON_VALUE"
    )
    df_out["TOTAL_CARBON"] = df_out["Count"] * df_out[base_col] * pixel_area_ha

    # 6. GENERATE RASTER DENSITY MAP (The "Painter")
    carbon_raster_path = out.parent / f"{country.lower()}_carbon_density.tif"
    carbon_map_dict = df_out.set_index(["LULC", "CZ_ID"])[base_col].to_dict()

    # Initialize with NaN and force float32 to avoid dtype conversion errors
    carbon_arr = np.full(lulc_arr.shape, np.nan, dtype="float32")

    for (l_val, cz_val), c_val in carbon_map_dict.items():
        pixel_mask = (lulc_arr == l_val) & (cz_raster == cz_val)
        if not np.isnan(c_val):
            carbon_arr[pixel_mask] = c_val

    # Create a clean float32 DataArray for output
    # This prevents rioxarray from trying to use the original uint8 settings
    out_rds = rds_proj.astype("float32")
    out_rds.values[0] = carbon_arr
    out_rds.rio.write_nodata(np.nan, inplace=True)
    out_rds.rio.to_raster(carbon_raster_path)

    # 7. OUTPUT AGGREGATION
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
    existing_cols = [c for c in cols if c in df_out.columns]
    write_dataframe(df_out[existing_cols], out)

    if out_gpkg:
        write_geopackage({"carbon_zones": gdf_zones_clip}, out_gpkg)

    logging.info(f"Success. Map with NoData borders saved to {carbon_raster_path}")
    return {"status": "success", "total_carbon_mg": df_out["TOTAL_CARBON"].sum()}
