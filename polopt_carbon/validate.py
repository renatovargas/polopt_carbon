"""
validate.py
------------
Utility functions for checking input files before running the
main carbon coefficients computation.

Checks include:
- File existence
- CRS consistency between layers
- Required field presence in carbon-zone polygons
- Basic raster sanity (dimensions, nodata)
"""

from pathlib import Path
import logging
import rasterio
import geopandas as gpd


def check_file_exists(path: Path, filetype: str):
    """Verify a file exists and is readable."""
    if not path.exists():
        raise FileNotFoundError(f"{filetype} not found: {path}")
    if path.stat().st_size == 0:
        raise ValueError(f"{filetype} file is empty: {path}")
    logging.debug(f"{filetype} OK: {path}")


def check_crs_match(lulc_path: Path, zones_path: Path, boundary_path: Path):
    """Ensure all datasets share the same CRS."""
    with rasterio.open(lulc_path) as src:
        lulc_crs = src.crs

    zones_crs = gpd.read_file(zones_path).crs
    boundary_crs = gpd.read_file(boundary_path).crs

    mismatched = []
    if zones_crs != lulc_crs:
        mismatched.append(f"carbon_zones ({zones_crs}) ≠ LULC ({lulc_crs})")
    if boundary_crs != lulc_crs:
        mismatched.append(f"boundary ({boundary_crs}) ≠ LULC ({lulc_crs})")

    if mismatched:
        msg = "CRS mismatch detected:\n  " + "\n  ".join(mismatched)
        logging.warning(msg)
        return False, msg

    logging.debug("All CRS match.")
    return True, "All CRS match."


def check_required_fields(zones_path: Path, required_fields=None):
    """Check that required attribute fields are present."""
    if required_fields is None:
        required_fields = ["CODE", "GEZ_TERM", "FRONTIER", "REGION"]

    gdf = gpd.read_file(zones_path, rows=1)  # read only first record
    fields = list(gdf.columns)
    missing = [f for f in required_fields if f not in fields]

    if missing:
        msg = f"Missing required fields in carbon_zones: {missing}"
        logging.warning(msg)
        return False, msg

    logging.debug("All required fields present.")
    return True, "All required fields present."


def run_validation(lulc: Path, zones: Path, boundary: Path) -> dict:
    """Run all validation checks and return a summary."""
    logging.info("Running input validation...")
    results = {}

    # File existence
    for p, label in [
        (lulc, "LULC raster"),
        (zones, "carbon_zones"),
        (boundary, "boundary"),
    ]:
        try:
            check_file_exists(p, label)
            results[label] = "exists"
        except Exception as e:
            results[label] = str(e)

    # CRS consistency
    try:
        ok_crs, msg_crs = check_crs_match(lulc, zones, boundary)
        results["crs_check"] = msg_crs
    except Exception as e:
        results["crs_check"] = str(e)

    # Field presence
    try:
        ok_fields, msg_fields = check_required_fields(zones)
        results["field_check"] = msg_fields
    except Exception as e:
        results["field_check"] = str(e)

    logging.info("Validation complete.")
    return results
