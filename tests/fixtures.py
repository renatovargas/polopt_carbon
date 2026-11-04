"""Deterministic synthetic inputs shared by the tests and the golden files."""

from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box


def write_inputs(folder: Path) -> tuple[Path, Path]:
    """Write a small LULC raster and boundary spanning several GEZ zones."""
    folder.mkdir(parents=True, exist_ok=True)
    lulc = folder / "lulc.tif"
    boundary = folder / "boundary.gpkg"
    size = 300
    classes = (np.indices((size, size)).sum(0) % 17 + 1).astype("uint8")
    classes[:5] = 255
    with rasterio.open(
        lulc,
        "w",
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(29.5, 4.5, 0.02, 0.02),
        nodata=255,
    ) as dst:
        dst.write(classes, 1)
    gpd.GeoDataFrame(geometry=[box(30.0, -1.0, 35.0, 4.0)], crs="EPSG:4326").to_file(
        boundary
    )
    return lulc, boundary
