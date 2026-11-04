"""InVEST carbon pool tables, which hold one value per land cover code."""

from __future__ import annotations

import pandas as pd

VALUE_COLUMN = "CARBON_VALUE_ADJ"
EXTRA_POOLS = ["c_below", "c_soil", "c_dead"]


def dominant_values(df: pd.DataFrame) -> pd.DataFrame:
    """Use the value of the largest overlay row for each class."""
    return df.sort_values("Count", ascending=False).drop_duplicates("LULC")[
        ["LULC", VALUE_COLUMN]
    ]


def weighted_values(df: pd.DataFrame) -> pd.DataFrame:
    """Use the pixel-weighted mean value for each class."""
    totals = df.assign(wt_val=df[VALUE_COLUMN] * df["Count"])
    totals = totals.groupby("LULC").agg({"wt_val": "sum", "Count": "sum"}).reset_index()
    totals[VALUE_COLUMN] = totals["wt_val"] / totals["Count"]
    return totals[["LULC", VALUE_COLUMN]]


def pool_table(
    values: pd.DataFrame, lucodes: list[int], all_pools: bool = False
) -> pd.DataFrame:
    """Build a table with every code, using 0 for classes absent from the raster."""
    table = (
        pd.DataFrame({"lucode": lucodes})
        .merge(
            values.rename(columns={"LULC": "lucode", VALUE_COLUMN: "c_above"}),
            on="lucode",
            how="left",
        )
        .fillna(0.0)[["lucode", "c_above"]]
    )
    if all_pools:
        for pool in EXTRA_POOLS:
            table[pool] = 0.0
    return table
