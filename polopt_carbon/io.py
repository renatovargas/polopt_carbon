"""
io.py
-----
Centralized input/output helpers for reading, writing, and metadata management.
Used by the core processing pipeline to abstract away file I/O details.
"""

from __future__ import annotations
from pathlib import Path
import logging
import json
import geopandas as gpd
import pandas as pd
import rasterio
from rasterio import Affine


# ---------------------------------------------------------------------------
# Reading utilities
# ---------------------------------------------------------------------------


def read_lulc_raster(lulc_path: Path):
    """Read a LULC GeoTIFF raster and return array + metadata."""
    logging.info(f"Reading LULC raster: {lulc_path}")
    with rasterio.open(lulc_path) as src:
        arr = src.read(1)
        meta = src.meta.copy()
        transform: Affine = src.transform
        crs = src.crs
    logging.debug(f"LULC shape: {arr.shape}, CRS: {crs}")
    return arr, meta, transform, crs


def read_vector(vector_path: Path, target_crs=None) -> gpd.GeoDataFrame:
    """Read a vector file (Shapefile or GPKG) and optionally reproject."""
    logging.info(f"Reading vector: {vector_path}")
    gdf = gpd.read_file(vector_path)
    if target_crs:
        gdf = gdf.to_crs(target_crs)
    logging.debug(f"Vector features: {len(gdf)}, CRS: {gdf.crs}")
    return gdf


# ---------------------------------------------------------------------------
# Writing utilities
# ---------------------------------------------------------------------------


def write_dataframe(df: pd.DataFrame, out_path: Path):
    """Write tabular outputs to CSV or Parquet, depending on suffix."""
    logging.info(f"Writing table: {out_path}")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.suffix == ".csv":
        df.to_csv(out_path, index=False)
    elif out_path.suffix in (".parquet", ".pq"):
        df.to_parquet(out_path, index=False)
    else:
        logging.warning("Unknown file extension, defaulting to CSV.")
        df.to_csv(out_path.with_suffix(".csv"), index=False)


def write_geopackage(gdf_dict: dict[str, gpd.GeoDataFrame], out_gpkg: Path):
    """
    Write one or more GeoDataFrames to layers in a GeoPackage.
    gdf_dict = {"layer_name": gdf}
    """
    logging.info(f"Writing GeoPackage: {out_gpkg}")
    out_gpkg.parent.mkdir(parents=True, exist_ok=True)

    for layer, gdf in gdf_dict.items():
        logging.debug(f"Writing layer '{layer}' with {len(gdf)} features")
        gdf.to_file(out_gpkg, layer=layer, driver="GPKG")


# ---------------------------------------------------------------------------
# Metadata utility
# ---------------------------------------------------------------------------


def write_run_metadata(metadata: dict, out_dir: Path):
    """Write a small JSON file capturing environment, CRS, and file info."""
    out_dir.mkdir(parents=True, exist_ok=True)
    meta_path = out_dir / "run_info.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)
    logging.debug(f"Wrote run metadata: {meta_path}")
    return meta_path


def read_coeff_lookup(path: Path) -> pd.DataFrame:
    """Read the R&G coefficient lookup CSV."""
    return pd.read_csv(path)
