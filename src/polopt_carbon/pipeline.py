"""The carbon pipeline: overlay land cover with carbon zones and write products."""

from __future__ import annotations

import hashlib
import json
import logging
import platform
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from importlib import resources
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import rioxarray
from rasterio import features
from rasterio.warp import transform_bounds

from polopt_carbon import __version__
from polopt_carbon.coefficients import (
    GEZ_LABEL_FIXES,
    SOURCE_UNMATCHED,
    attach_coefficients,
    global_values,
    load_crosswalk,
    load_lookup,
)
from polopt_carbon.config import InputError, OutputExistsError, RunConfig
from polopt_carbon.invest import dominant_values, pool_table, weighted_values

logger = logging.getLogger(__name__)

TARGET_CRS = "EPSG:6933"
LULC_FILL_LIMIT = 255
WATER_CODE = 17
TABLE_COLUMNS = {
    "ISO3": "ISO3",
    "YEAR": "YEAR",
    "LULC": "LULC",
    "LULC_CLASS": "LULC_CLASS",
    "REGION": "REGION",
    "GEZ_TERM": "GEZ_TERM",
    "FRONTIER": "FRONTIER",
    "GLC2000": "GLC2000",
    "CARBON_VALUE": "CARBON_VALUE",
    "rule_applied": "RULE",
    "CARBON_VALUE_ADJ": "CARBON_VALUE_USED",
    "Count": "PIXEL_COUNT",
    "PIXEL_AREA_HA": "PIXEL_AREA_HA",
    "TOTAL_CARBON": "TOTAL_CARBON",
    "VALUE_SOURCE": "VALUE_SOURCE",
}

# The remaining columns identify each aggregated row.
AGGREGATIONS = {
    "PIXEL_COUNT": "sum",
    "CARBON_VALUE": "max",
    "CARBON_VALUE_USED": "max",
    "TOTAL_CARBON": "sum",
}

ANALYSIS_STAGES = [
    "Checking inputs",
    "Reprojecting land cover",
    "Overlaying carbon zones",
    "Assigning carbon values",
]
WRITE_STAGES = ["Writing rasters", "Writing tables", "Writing run manifest"]

StageCallback = Callable[[str], None]


@dataclass(frozen=True)
class Check:
    name: str
    passed: bool
    detail: str


@dataclass
class Analysis:
    table: pd.DataFrame
    zones: gpd.GeoDataFrame
    lulc: np.ndarray
    zone_ids: np.ndarray
    mask: np.ndarray
    transform: Any
    nodata: float | None
    pixel_area_ha: float
    lucodes: list[int]
    warnings: list[str]
    checks: list[Check]

    @property
    def total_carbon(self) -> float:
        return float(self.table["TOTAL_CARBON"].sum())


@dataclass
class RunResult:
    analysis: Analysis
    outputs: dict[str, Path] = field(default_factory=dict)
    manifest: Path | None = None


class _StopChecks(Exception):
    pass


def check_inputs(cfg: RunConfig) -> list[Check]:
    """Check that the inputs are readable and overlap, stopping at the first failure."""
    checks = []

    def record(name: str, passed: bool, detail: str) -> None:
        checks.append(Check(name, passed, detail))
        if not passed:
            raise _StopChecks

    try:
        if not cfg.lulc.is_file():
            record("LULC raster", False, f"File not found: {cfg.lulc}")
        try:
            with rasterio.open(cfg.lulc) as src:
                crs, bounds, shape = src.crs, src.bounds, src.shape
                count, dtype = src.count, src.dtypes[0]
        except rasterio.RasterioIOError as exc:
            record("LULC raster", False, f"Cannot read {cfg.lulc}: {exc}")
        record(
            "LULC raster",
            crs is not None,
            f"{shape[1]} x {shape[0]} px, {dtype}, {crs}"
            if crs
            else "The raster has no coordinate system",
        )
        if count > 1:
            checks.append(
                Check("LULC bands", True, f"{count} bands found, using only band 1")
            )

        if not cfg.boundary.exists():
            record("Boundary", False, f"File not found: {cfg.boundary}")
        try:
            boundary = gpd.read_file(cfg.boundary)
        except Exception as exc:  # noqa: BLE001 - drivers raise many error types
            record("Boundary", False, f"Cannot read {cfg.boundary}: {exc}")
        record("Boundary", not boundary.empty, f"{len(boundary)} feature(s)")
        record(
            "Boundary CRS",
            boundary.crs is not None,
            str(boundary.crs)
            if boundary.crs
            else "The boundary has no coordinate system",
        )

        lon_min, lat_min, lon_max, lat_max = transform_bounds(crs, "EPSG:4326", *bounds)
        b_lon_min, b_lat_min, b_lon_max, b_lat_max = boundary.to_crs(
            "EPSG:4326"
        ).total_bounds
        overlap = (
            lon_min < b_lon_max
            and b_lon_min < lon_max
            and lat_min < b_lat_max
            and b_lat_min < lat_max
        )
        record(
            "Overlap",
            bool(overlap),
            "The raster and boundary overlap"
            if overlap
            else "The raster and boundary do not overlap",
        )
        for label, path in (
            ("Custom lookup", cfg.coeff_lookup),
            ("Custom crosswalk", cfg.crosswalk),
        ):
            if path is not None:
                record(label, path.is_file(), str(path))
    except _StopChecks:
        pass
    return checks


def _coverage_warnings(
    df: pd.DataFrame, lookup: pd.DataFrame, area: float
) -> list[str]:
    """Explain rows that had no lookup match or rule and so received 0."""
    unmatched = df[df["VALUE_SOURCE"] == SOURCE_UNMATCHED]
    available = global_values(lookup)
    has_global = unmatched["GLC2000"].isin(available)
    messages = []

    positive = unmatched[has_global & unmatched["GLC2000"].map(available).gt(0)]
    for (code, name, label), rows in positive.groupby(
        ["LULC", "LULC_CLASS", "GLC2000"]
    ):
        pixels = int(rows["Count"].sum())
        value = available[label]
        messages.append(
            f"{name} (class {code}): {pixels:,} px set to 0, but the lookup's Global "
            f"row has {value:g} t C/ha ({pixels * area * value / 1e6:,.2f} Mt C). "
            "--use-global-classes applies it."
        )

    typo_terms = list(GEZ_LABEL_FIXES.values())
    typo_labels = lookup.loc[lookup["GEZ_TERM"].isin(GEZ_LABEL_FIXES), "GLC2000"]
    typo = unmatched["GEZ_TERM"].isin(typo_terms) & unmatched["GLC2000"].isin(
        typo_labels
    )
    if typo.any():
        messages.append(
            f"{int(unmatched.loc[typo, 'Count'].sum()):,} px in "
            f"'{typo_terms[0]}' zones set to 0 because of a label typo in the lookup. "
            "--use-global-classes fixes it."
        )

    no_class = unmatched[unmatched["LULC_CLASS"].isna()]
    if not no_class.empty:
        codes = ", ".join(str(c) for c in sorted(no_class["LULC"].unique()))
        messages.append(
            f"{int(no_class['Count'].sum()):,} px use land cover codes missing from "
            f"the crosswalk ({codes}) and were set to 0."
        )

    other = unmatched[
        ~has_global
        & ~typo
        & unmatched["LULC_CLASS"].notna()
        & (unmatched["LULC"] != WATER_CODE)
    ]
    if not other.empty:
        terms = ", ".join(sorted(other["GEZ_TERM"].astype(str).unique()))
        messages.append(
            f"{int(other['Count'].sum()):,} px in zones without lookup values "
            f"({terms}) were set to 0."
        )
    return messages


def analyze(cfg: RunConfig, on_stage: StageCallback | None = None) -> Analysis:
    """Run all calculations without writing any files."""
    stage = on_stage or (lambda _: None)

    stage(ANALYSIS_STAGES[0])
    checks = check_inputs(cfg)
    failed = [c for c in checks if not c.passed]
    if failed:
        raise InputError(f"{failed[0].name}: {failed[0].detail}")
    crosswalk = load_crosswalk(cfg.crosswalk)
    lookup = load_lookup(cfg.coeff_lookup)

    stage(ANALYSIS_STAGES[1])
    source = resources.files("polopt_carbon.data").joinpath("gez.gpkg")
    with resources.as_file(source) as gez_path:
        zones = gpd.read_file(gez_path).to_crs(TARGET_CRS)
    boundary = gpd.read_file(cfg.boundary).to_crs(TARGET_CRS)
    with rioxarray.open_rasterio(cfg.lulc) as raster:
        projected = raster.rio.reproject(TARGET_CRS)
    lulc = projected.values[0]
    transform = projected.rio.transform()
    nodata = projected.rio.nodata
    pixel_area_ha = abs(transform.a * transform.e) / 10000.0

    stage(ANALYSIS_STAGES[2])
    zones_clip = gpd.clip(zones, boundary).reset_index(drop=True)
    shapes = ((geom, idx + 1) for idx, geom in enumerate(zones_clip.geometry))
    zone_ids = features.rasterize(
        shapes=shapes,
        out_shape=lulc.shape,
        transform=transform,
        fill=0,
        dtype="int32",
    )
    mask = (zone_ids > 0) & (~np.isnan(lulc)) & (lulc < LULC_FILL_LIMIT)
    if nodata is not None and not np.isnan(nodata):
        mask &= lulc != nodata
    if not mask.any():
        raise InputError(
            "No valid land cover pixels fall inside the boundary. "
            "Check that the raster and boundary cover the same area."
        )

    stage(ANALYSIS_STAGES[3])
    counts = pd.DataFrame(
        {"LULC": lulc[mask].astype(int), "CZ_ID": zone_ids[mask].astype(int)}
    )
    counts = counts.groupby(["LULC", "CZ_ID"]).size().reset_index(name="Count")
    attrs = zones_clip.drop(columns=["geometry"], errors="ignore").reset_index()
    attrs = attrs.rename(columns={"index": "CZ_ID"})
    attrs["CZ_ID"] += 1
    counts = counts.merge(
        attrs[["CZ_ID", "REGION", "GEZ_TERM", "FRONTIER"]], on="CZ_ID", how="left"
    )
    table = attach_coefficients(
        counts, crosswalk, lookup, cfg.rules, cfg.use_global_classes
    )
    table["TOTAL_CARBON"] = table["Count"] * table["CARBON_VALUE_ADJ"] * pixel_area_ha
    table["ISO3"] = cfg.iso
    table["YEAR"] = cfg.year
    table["PIXEL_AREA_HA"] = pixel_area_ha

    warnings = list(cfg.notices)
    warnings += _coverage_warnings(table, lookup, pixel_area_ha)
    if cfg.crosswalk is not None:
        warnings.append(
            "Fallback rules use MODIS/IGBP class codes. Check that they suit your "
            "custom crosswalk."
        )
    if cfg.use_global_classes:
        warnings.append(
            "--use-global-classes changes results compared with the default method."
        )
    return Analysis(
        table=table,
        zones=zones_clip,
        lulc=lulc,
        zone_ids=zone_ids,
        mask=mask,
        transform=transform,
        nodata=nodata,
        pixel_area_ha=pixel_area_ha,
        lucodes=[int(code) for code in sorted(crosswalk["LULC"].unique())],
        warnings=warnings,
        checks=checks,
    )


def density_array(analysis: Analysis) -> np.ndarray:
    """Paint each overlay row's value onto its pixels; everything else is NaN."""
    table = analysis.table
    scale = int(analysis.zone_ids.max()) + 1
    keys = table["LULC"].to_numpy(dtype="int64") * scale + table["CZ_ID"].to_numpy(
        dtype="int64"
    )
    order = np.argsort(keys)
    keys, values = keys[order], table["CARBON_VALUE_ADJ"].to_numpy()[order]

    pixel_keys = (
        analysis.lulc[analysis.mask].astype("int64") * scale
        + analysis.zone_ids[analysis.mask]
    )
    density = np.full(analysis.lulc.shape, np.nan, dtype="float32")
    density[analysis.mask] = values[np.searchsorted(keys, pixel_keys)]
    return density


def output_paths(cfg: RunConfig) -> dict[str, Path]:
    stem = f"{cfg.iso.lower()}_{cfg.year}"
    folder = cfg.output_dir
    return {
        "carbon_density": folder / f"{stem}_carbon_density.tif",
        "lulc": folder / f"{stem}_lulc_6933.tif",
        "carbon_table": folder / f"{stem}_carbon_table.xlsx",
        "invest_dominant": folder / f"{stem}_invest_carbon_table_dominant.csv",
        "invest_weighted": folder / f"{stem}_invest_carbon_table_weighted.csv",
        "gez": folder / f"{stem}_gez.gpkg",
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_raster(
    path: Path,
    values: np.ndarray,
    analysis: Analysis,
    nodata: float | None,
    description: str,
) -> None:
    height, width = values.shape
    predictor = 3 if np.issubdtype(values.dtype, np.floating) else 2
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype=values.dtype,
        crs=TARGET_CRS,
        transform=analysis.transform,
        nodata=nodata,
        compress="deflate",
        predictor=predictor,
    ) as dst:
        dst.write(values, 1)
        dst.set_band_description(1, description)
        dst.update_tags(software=f"polopt-carbon {__version__}")


def aggregate_table(export: pd.DataFrame) -> pd.DataFrame:
    """Combine the zone polygons that share all other attributes.

    Blank keys must be kept. Savannas have no GLC2000 class, for example, so
    dropping blanks would remove them and their carbon.
    """
    keys = [column for column in export.columns if column not in AGGREGATIONS]
    grouped = export.groupby(keys, dropna=False, as_index=False).agg(AGGREGATIONS)
    return grouped[list(export.columns)]


def _write_carbon_table(path: Path, export: pd.DataFrame) -> None:
    """Write both sheets, opening on the aggregated one."""
    with pd.ExcelWriter(path) as writer:
        export.to_excel(
            writer, sheet_name="By polygon", index=False, freeze_panes=(1, 0)
        )
        aggregate_table(export).to_excel(
            writer, sheet_name="Aggregated", index=False, freeze_panes=(1, 0)
        )
        writer.book.active = 1
        writer.sheets["By polygon"].sheet_view.tabSelected = False


def run(cfg: RunConfig, on_stage: StageCallback | None = None) -> RunResult:
    """Run the pipeline and write all products."""
    paths = output_paths(cfg)
    existing = [p for p in paths.values() if p.exists()]
    if existing and not cfg.overwrite:
        raise OutputExistsError(
            f"{len(existing)} output file(s) already exist in {cfg.output_dir}. "
            "Use --overwrite to replace them or -o to choose another folder."
        )

    analysis = analyze(cfg, on_stage)
    stage = on_stage or (lambda _: None)
    cfg.output_dir.mkdir(parents=True, exist_ok=True)

    stage(WRITE_STAGES[0])
    _write_raster(
        paths["carbon_density"],
        density_array(analysis),
        analysis,
        float("nan"),
        "carbon density (t C/ha)",
    )
    _write_raster(
        paths["lulc"], analysis.lulc, analysis, analysis.nodata, "land cover class"
    )

    stage(WRITE_STAGES[1])
    export = analysis.table.rename(columns=TABLE_COLUMNS)[list(TABLE_COLUMNS.values())]
    _write_carbon_table(paths["carbon_table"], export)
    table = analysis.table
    for key, values in (
        ("invest_dominant", dominant_values(table)),
        ("invest_weighted", weighted_values(table)),
    ):
        pool_table(values, analysis.lucodes, cfg.invest_all_pools).to_csv(
            paths[key], index=False
        )
    analysis.zones.to_file(paths["gez"], layer="carbon_zones", driver="GPKG")

    stage(WRITE_STAGES[2])
    manifest = _write_manifest(cfg, analysis, paths)
    logger.info("Wrote %s", manifest)
    return RunResult(analysis, paths, manifest)


def _write_manifest(cfg: RunConfig, analysis: Analysis, paths: dict[str, Path]) -> Path:
    path = cfg.output_dir / f"{cfg.iso.lower()}_{cfg.year}_manifest.json"
    payload = {
        "software": {
            "polopt_carbon": __version__,
            "python": platform.python_version(),
            "gdal": rasterio.__gdal_version__,
            "rasterio": rasterio.__version__,
            "geopandas": gpd.__version__,
            "pandas": pd.__version__,
            "numpy": np.__version__,
        },
        "created": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "settings": cfg.to_dict(),
        "inputs": {
            "lulc": {"path": str(cfg.lulc), "sha256": _sha256(cfg.lulc)},
            "boundary": {"path": str(cfg.boundary), "sha256": _sha256(cfg.boundary)},
        },
        "results": {
            "total_carbon_mg": analysis.total_carbon,
            "pixel_area_ha": analysis.pixel_area_ha,
            "value_sources": {
                source: int(pixels)
                for source, pixels in analysis.table.groupby("VALUE_SOURCE")["Count"]
                .sum()
                .items()
            },
        },
        "warnings": analysis.warnings,
        "outputs": {
            key: {"path": str(value), "sha256": _sha256(value)}
            for key, value in paths.items()
        },
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def class_summary(table: pd.DataFrame) -> pd.DataFrame:
    """Summarize carbon and value sources by land cover class."""
    summary = (
        table.assign(LULC_CLASS=table["LULC_CLASS"].fillna("Unknown"))
        .groupby(["LULC", "LULC_CLASS"])
        .agg(pixels=("Count", "sum"), carbon=("TOTAL_CARBON", "sum"))
        .reset_index()
    )
    summary["area_ha"] = summary["pixels"] * table["PIXEL_AREA_HA"].iloc[0]
    summary["density"] = summary["carbon"] / summary["area_ha"]
    sources = (
        table.groupby("LULC")["VALUE_SOURCE"]
        .agg(lambda s: ", ".join(sorted(set(s))))
        .rename("sources")
    )
    return summary.merge(sources, on="LULC")
