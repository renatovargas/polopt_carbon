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
from rasterio import features
import rioxarray
from importlib import resources

from polopt_carbon.io import write_dataframe, write_geopackage
from polopt_carbon.rules import apply_fallback_rules

# Equal-area projection for consistent carbon density calculations
TARGET_CRS = "EPSG:6933"


def compute(
    country: str,
    lulc: Path,
    zones: Path,
    boundary: Path,
    output_dir: Path,
    overwrite: bool = False,
    coeff_lookup: Path | None = None,
    crosswalk_path: Path | None = None,
    method: str = "dominant",
    expert_rules: Path | None = None,
    force_wetland_overrides: bool = False,
) -> dict:
    logging.info(f"Starting compute() for {country}")

    # 0. SETUP OUTPUT DIRECTORY AND FILENAMES
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    iso = country.lower()
    out_table = out_path / f"{iso}_carbon_table.csv"
    out_invest = out_path / f"{iso}_invest_carbon_table.csv"
    out_raster = out_path / f"{iso}_carbon_density.tif"
    out_gez_gpkg = out_path / f"{iso}_gez.gpkg"

    # 1. PRE-REPROJECTION
    gdf_zones = gpd.read_file(zones).to_crs(TARGET_CRS)
    gdf_boundary = gpd.read_file(boundary).to_crs(TARGET_CRS)

    rds = rioxarray.open_rasterio(lulc)
    rds_proj = rds.rio.reproject(TARGET_CRS)

    lulc_arr = rds_proj.values[0]
    lulc_transform = rds_proj.rio.transform()

    # Calculate Pixel Area in Hectares (EPSG:6933 is in meters)
    pixel_area_ha = abs(lulc_transform.a * lulc_transform.e) / 10000.0

    # 2. SPATIAL LOGIC (Zonal Rasterization)
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
    mask = (cz_raster > 0) & (~np.isnan(lulc_arr)) & (lulc_arr < 255)
    df_out = pd.DataFrame(
        {"LULC": lulc_arr[mask].astype(int), "CZ_ID": cz_raster[mask].astype(int)}
    )
    df_out = df_out.groupby(["LULC", "CZ_ID"]).size().reset_index(name="Count")

    # 4. ATTRIBUTE JOINS
    attrs = gdf_zones_clip.drop(columns=["geometry"], errors="ignore").reset_index()
    attrs = attrs.rename(columns={"index": "CZ_ID"})
    attrs["CZ_ID"] += 1
    df_out = df_out.merge(
        attrs[["CZ_ID", "REGION", "GEZ_TERM", "FRONTIER"]], on="CZ_ID", how="left"
    )

    # Load Crosswalk & Lookup
    cw_path = crosswalk_path or resources.files("polopt_carbon.data").joinpath(
        "modis_glc_crosswalk.csv"
    )
    df_out = df_out.merge(
        pd.read_csv(cw_path)[["LULC", "LULC_CLASS", "GLC2000 Class"]],
        on="LULC",
        how="left",
    )
    df_out = df_out.rename(columns={"GLC2000 Class": "GLC2000"})

    lut_path = coeff_lookup or resources.files("polopt_carbon.data").joinpath(
        "rg_lookup_exploded.csv"
    )
    df_out = df_out.merge(
        pd.read_csv(lut_path),
        on=["GLC2000", "REGION", "GEZ_TERM", "FRONTIER"],
        how="left",
    )

    # 5. APPLY RULES & CALCULATE UNITS
    # RESTORED RULE CONFIGURATION
    rule_cfg = {
        "savanna_percent_of_forest": 0.4,
        "wetland_woody_equals_forest": True,
        "marsh_equals_shrub": True,
        "force_wetland_overrides": force_wetland_overrides,
    }
    df_out = apply_fallback_rules(df_out, config=rule_cfg)

    # Use adjusted column if rule was applied, otherwise base column
    base_col = (
        "CARBON_VALUE_ADJ" if "CARBON_VALUE_ADJ" in df_out.columns else "CARBON_VALUE"
    )
    df_out[base_col] = df_out[base_col].fillna(0.0)
    df_out["TOTAL_CARBON"] = df_out["Count"] * df_out[base_col] * pixel_area_ha

    # 6. GENERATE RASTER DENSITY MAP
    carbon_map_dict = df_out.set_index(["LULC", "CZ_ID"])[base_col].to_dict()
    carbon_arr = np.full(lulc_arr.shape, np.nan, dtype="float32")
    for (l_val, cz_val), c_val in carbon_map_dict.items():
        carbon_arr[(lulc_arr == l_val) & (cz_raster == cz_val)] = c_val

    out_rds = rds_proj.astype("float32")
    out_rds.values[0] = carbon_arr
    out_rds.rio.write_nodata(np.nan, inplace=True)
    out_rds.rio.to_raster(out_raster)

    # 7. EXPORT STANDARDIZED TABLE & GEOPACKAGE
    write_dataframe(df_out, out_table)
    write_geopackage({"carbon_zones": gdf_zones_clip}, out_gez_gpkg)

    # 8. GENERATE 2-COLUMN InVEST TABLE (Seeded 1-17)
    if method == "dominant":
        invest_agg = df_out.sort_values("Count", ascending=False).drop_duplicates(
            "LULC"
        )
    else:
        # Weighted average approach
        df_out["wt_val"] = df_out[base_col] * df_out["Count"]
        agg_res = (
            df_out.groupby("LULC").agg({"wt_val": "sum", "Count": "sum"}).reset_index()
        )
        agg_res[base_col] = agg_res["wt_val"] / agg_res["Count"]
        invest_agg = agg_res[["LULC", base_col]]

    # Ensure exactly 17 rows and only required columns
    all_codes = pd.DataFrame({"lucode": range(1, 18)})
    invest_final = all_codes.merge(
        invest_agg[["LULC", base_col]].rename(
            columns={"LULC": "lucode", base_col: "c_above"}
        ),
        on="lucode",
        how="left",
    ).fillna(0.0)

    # Strictly save only 'lucode' and 'c_above'
    invest_final[["lucode", "c_above"]].to_csv(out_invest, index=False)

    logging.info(f"Success. All outputs saved to {out_path}")
    return {"status": "success", "total_carbon_mg": df_out["TOTAL_CARBON"].sum()}
