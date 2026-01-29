from pathlib import Path
import logging
from datetime import datetime
import geopandas as gpd
import pandas as pd
import numpy as np
from rasterio import features
import rioxarray
from importlib import resources

from polopt_carbon.io import write_dataframe, write_geopackage
from polopt_carbon.rules import apply_fallback_rules

TARGET_CRS = "EPSG:6933"


def compute(
    country: str,
    lulc: Path,
    boundary: Path,
    output_dir: Path,
    overwrite: bool = False,
    coeff_lookup: Path | None = None,
    crosswalk_path: Path | None = None,
    method: str = "dominant",
    expert_rules: Path | None = None,
    force_wetland_overrides: bool = False,
    year: int | None = None,
) -> dict:
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    # 1. Add Year Logic
    if year is None:
        year = datetime.now().year

    iso = country.lower()

    # 2. Change naming convention to iso_year_output.extension
    # CHANGED: Output table is now .xlsx
    out_table = out_path / f"{iso}_{year}_carbon_table.xlsx"

    # CHANGED: Define two output paths for InVEST tables
    out_invest_dom = out_path / f"{iso}_{year}_invest_carbon_table_dominant.csv"
    out_invest_wgt = out_path / f"{iso}_{year}_invest_carbon_table_weighted.csv"

    out_raster = out_path / f"{iso}_{year}_carbon_density.tif"
    out_gez_gpkg = out_path / f"{iso}_{year}_gez.gpkg"

    # Internalized GEZ load - sourced from package data folder
    with resources.files("polopt_carbon.data").joinpath("gez.gpkg") as gez_path:
        gdf_zones = gpd.read_file(gez_path).to_crs(TARGET_CRS)

    gdf_boundary = gpd.read_file(boundary).to_crs(TARGET_CRS)
    rds = rioxarray.open_rasterio(lulc)
    rds_proj = rds.rio.reproject(TARGET_CRS)
    lulc_arr = rds_proj.values[0]
    lulc_transform = rds_proj.rio.transform()
    pixel_area_ha = abs(lulc_transform.a * lulc_transform.e) / 10000.0

    gdf_zones_clip = gpd.clip(gdf_zones, gdf_boundary).reset_index(drop=True)
    shapes = ((geom, idx + 1) for idx, geom in enumerate(gdf_zones_clip.geometry))
    cz_raster = features.rasterize(
        shapes=shapes,
        out_shape=lulc_arr.shape,
        transform=lulc_transform,
        fill=0,
        dtype="int32",
    )

    mask = (cz_raster > 0) & (~np.isnan(lulc_arr)) & (lulc_arr < 255)
    df_out = pd.DataFrame(
        {"LULC": lulc_arr[mask].astype(int), "CZ_ID": cz_raster[mask].astype(int)}
    )
    df_out = df_out.groupby(["LULC", "CZ_ID"]).size().reset_index(name="Count")

    attrs = gdf_zones_clip.drop(columns=["geometry"], errors="ignore").reset_index()
    attrs = attrs.rename(columns={"index": "CZ_ID"})
    attrs["CZ_ID"] += 1
    df_out = df_out.merge(
        attrs[["CZ_ID", "REGION", "GEZ_TERM", "FRONTIER"]], on="CZ_ID", how="left"
    )

    cw_p = crosswalk_path or resources.files("polopt_carbon.data").joinpath(
        "modis_glc_crosswalk.csv"
    )
    df_out = df_out.merge(
        pd.read_csv(cw_p)[["LULC", "LULC_CLASS", "GLC2000 Class"]],
        on="LULC",
        how="left",
    )
    df_out = df_out.rename(columns={"GLC2000 Class": "GLC2000"})

    lut_p = coeff_lookup or resources.files("polopt_carbon.data").joinpath(
        "rg_lookup_exploded.csv"
    )
    df_out = df_out.merge(
        pd.read_csv(lut_p), on=["GLC2000", "REGION", "GEZ_TERM", "FRONTIER"], how="left"
    )

    # RESTORED RULE CONFIGURATION
    rule_cfg = {
        "savanna_percent_of_forest": 0.4,
        "wetland_woody_equals_forest": True,
        "marsh_equals_shrub": True,
        "force_wetland_overrides": force_wetland_overrides,
    }
    df_out = apply_fallback_rules(df_out, config=rule_cfg)

    base_col = (
        "CARBON_VALUE_ADJ" if "CARBON_VALUE_ADJ" in df_out.columns else "CARBON_VALUE"
    )
    df_out[base_col] = df_out[base_col].fillna(0.0)
    df_out["TOTAL_CARBON"] = df_out["Count"] * df_out[base_col] * pixel_area_ha

    # 3. Add new fields to the main output table
    df_out["ISO3"] = iso.upper()
    df_out["YEAR"] = year
    df_out["PIXEL_AREA_HA"] = pixel_area_ha

    carbon_map_dict = df_out.set_index(["LULC", "CZ_ID"])[base_col].to_dict()
    carbon_arr = np.full(lulc_arr.shape, np.nan, dtype="float32")
    for (l_val, cz_val), c_val in carbon_map_dict.items():
        carbon_arr[(lulc_arr == l_val) & (cz_raster == cz_val)] = c_val

    out_rds = rds_proj.astype("float32")
    out_rds.values[0] = carbon_arr
    out_rds.rio.to_raster(out_raster)

    # -----------------------------------------------------------------------
    # Items 6, 7, 8: Rename, Reorder, and Drop CZ_ID for the Main Table only
    # We use a copy (df_export) so we don't break the InVEST logic below
    # which relies on "Count" and base_col names.
    # -----------------------------------------------------------------------
    df_export = df_out.copy()

    # 6. Rename columns
    rename_map = {
        "Count": "PIXEL_COUNT",
        base_col: "CARBON_VALUE_USED",
        "rule_applied": "RULE",
    }
    df_export = df_export.rename(columns=rename_map)

    # 7 & 8. Reorder columns (Implicitly drops CZ_ID by not listing it)
    final_cols = [
        "ISO3",
        "YEAR",
        "LULC",
        "LULC_CLASS",
        "REGION",
        "GEZ_TERM",
        "FRONTIER",
        "GLC2000",
        "CARBON_VALUE",
        "RULE",
        "CARBON_VALUE_USED",
        "PIXEL_COUNT",
        "PIXEL_AREA_HA",
        "TOTAL_CARBON",
    ]

    # Select only the columns that exist (safe-guard) in the desired order
    df_export = df_export[[c for c in final_cols if c in df_export.columns]]

    # CHANGED: Write to Excel directly (bypassing io.py CSV default)
    df_export.to_excel(out_table, index=False)

    write_geopackage({"carbon_zones": gdf_zones_clip}, out_gez_gpkg)

    # -----------------------------------------------------------------------
    # InVEST Table Logic (Generates BOTH methods regardless of input)
    # -----------------------------------------------------------------------

    # 1. Dominant Method
    invest_agg_dom = df_out.sort_values("Count", ascending=False).drop_duplicates(
        "LULC"
    )

    pd.DataFrame({"lucode": range(1, 18)}).merge(
        invest_agg_dom[["LULC", base_col]].rename(
            columns={"LULC": "lucode", base_col: "c_above"}
        ),
        on="lucode",
        how="left",
    ).fillna(0.0)[["lucode", "c_above"]].to_csv(out_invest_dom, index=False)

    # 2. Weighted Method
    # We calculate the weighted mean. Note: this adds 'wt_val' to df_out,
    # but since this is the final step, modifying df_out is acceptable.
    df_out["wt_val"] = df_out[base_col] * df_out["Count"]
    agg_res = (
        df_out.groupby("LULC").agg({"wt_val": "sum", "Count": "sum"}).reset_index()
    )
    agg_res[base_col] = agg_res["wt_val"] / agg_res["Count"]
    invest_agg_wgt = agg_res[["LULC", base_col]]

    pd.DataFrame({"lucode": range(1, 18)}).merge(
        invest_agg_wgt[["LULC", base_col]].rename(
            columns={"LULC": "lucode", base_col: "c_above"}
        ),
        on="lucode",
        how="left",
    ).fillna(0.0)[["lucode", "c_above"]].to_csv(out_invest_wgt, index=False)

    return {"status": "success", "total_carbon_mg": df_out["TOTAL_CARBON"].sum()}
