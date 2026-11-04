"""Compare two output folders, so new runs can be checked against delivered ones."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio

from polopt_carbon.config import InputError

TOLERANCE = 1e-4


@dataclass(frozen=True)
class Comparison:
    item: str
    same: bool
    detail: str


def _find(folder: Path, patterns: list[str]) -> Path | None:
    for pattern in patterns:
        matches = sorted(folder.glob(pattern))
        if matches:
            return matches[0]
    return None


def _read_density(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        values = src.read(1).astype("float64")
        # Files from v0.1.0 store NaN but declare 255 as nodata.
        if src.nodata is not None and not np.isnan(src.nodata):
            values[values == src.nodata] = np.nan
    return values


def _compare_density(old: Path, new: Path) -> Comparison:
    a, b = _read_density(old), _read_density(new)
    if a.shape != b.shape:
        return Comparison("Density raster", False, f"Size {a.shape} vs {b.shape}")
    both = np.isfinite(a) & np.isfinite(b)
    coverage = int((np.isfinite(a) != np.isfinite(b)).sum())
    changed = int((np.abs(a[both] - b[both]) > TOLERANCE).sum())
    same = coverage == 0 and changed == 0
    return Comparison(
        "Density raster",
        same,
        f"{int(both.sum()):,} px compared, {changed:,} differ, "
        f"{coverage:,} differ in coverage",
    )


def _read_table(path: Path) -> pd.DataFrame:
    table = pd.read_excel(path) if path.suffix == ".xlsx" else pd.read_csv(path)
    return table.rename(columns={"Count": "PIXEL_COUNT"})


def _compare_table(old: Path, new: Path) -> list[Comparison]:
    a, b = _read_table(old), _read_table(new)
    required = {"LULC", "PIXEL_COUNT", "TOTAL_CARBON"}
    for path, table in ((old, a), (new, b)):
        if not required <= set(table.columns):
            raise InputError(f"{path} is not a carbon table.")
    total_a, total_b = a["TOTAL_CARBON"].sum(), b["TOTAL_CARBON"].sum()
    gap = abs(total_a - total_b)
    results = [
        Comparison(
            "Total carbon",
            gap <= max(1.0, abs(total_a) * 1e-9),
            f"{total_a:,.0f} vs {total_b:,.0f} t C (difference {total_b - total_a:,.0f})",
        )
    ]
    by_class = pd.DataFrame(
        {
            "old": a.groupby("LULC")["TOTAL_CARBON"].sum(),
            "new": b.groupby("LULC")["TOTAL_CARBON"].sum(),
        }
    ).fillna(0.0)
    changed = by_class[(by_class["old"] - by_class["new"]).abs() > 1.0]
    results.append(
        Comparison(
            "Carbon by class",
            changed.empty,
            "All classes match"
            if changed.empty
            else "Classes differ: " + ", ".join(str(code) for code in changed.index),
        )
    )
    return results


def _compare_invest(label: str, old: Path, new: Path) -> Comparison:
    a = pd.read_csv(old).set_index("lucode")
    b = pd.read_csv(new).set_index("lucode")
    if not a.index.equals(b.index):
        return Comparison(label, False, "The tables list different land cover codes")
    gap = (a["c_above"] - b["c_above"]).abs()
    changed = [str(code) for code in gap[gap > TOLERANCE].index]
    return Comparison(
        label,
        not changed,
        "All codes match" if not changed else "Codes differ: " + ", ".join(changed),
    )


def compare_folders(old: Path, new: Path) -> list[Comparison]:
    for folder in (old, new):
        if not folder.is_dir():
            raise InputError(f"Folder not found: {folder}")

    results = []
    pairs = {
        "density": ["*carbon_density.tif"],
        "table": ["*_carbon_table.xlsx", "*_carbon_table.csv"],
    }
    for kind, patterns in pairs.items():
        a, b = _find(old, patterns), _find(new, patterns)
        if a is None or b is None:
            results.append(
                Comparison(kind.capitalize(), False, "Not found in both folders")
            )
        elif kind == "density":
            results.append(_compare_density(a, b))
        else:
            results.extend(_compare_table(a, b))

    # Files from before v0.1.0 have one table, written with the dominant method.
    invest_tables = (
        ("InVEST dominant", ["*invest_carbon_table_dominant.csv"]),
        ("InVEST weighted", ["*invest_carbon_table_weighted.csv"]),
    )
    for label, patterns in invest_tables:
        a, b = _find(old, patterns), _find(new, patterns)
        if a is None and label == "InVEST dominant":
            a = _find(old, ["*invest_carbon_table.csv"])
        if a is not None and b is not None:
            results.append(_compare_invest(label, a, b))
    return results
