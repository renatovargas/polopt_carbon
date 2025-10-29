import pandas as pd
from pathlib import Path

# ---------------------------------------------------------------------
# 1. Input / output paths
# ---------------------------------------------------------------------
in_path = Path("~/testdata/uga/lookup/rg_to_explode.csv").expanduser()
out_path = Path("~/testdata/uga/lookup/rg_lookup_exploded.csv").expanduser()

# ---------------------------------------------------------------------
# 2. Mapping of R&G regions to actual region names in your shapefile
# ---------------------------------------------------------------------
region_map = {
    "Africa": "Africa",
    "N and S America": "North America;South America",
    "N America": "North America",
    "S America": "South America",
    "Continental Asia": "Asia",
    "Insular Asia": "Asia (insular)",
    "Asia and Europe": "Asia;Europe",
    "Europe": "Europe",
    "Australia": "Australia",
    "Australia and New Zealand": "Australia;New Zealand",
    "Global": ";".join(
        [
            "Africa",
            "Asia",
            "Asia (insular)",
            "Europe",
            "North America",
            "South America",
            "Australia",
            "New Zealand",
            "Antarctica",
            "Pacific Ocean",
            "Atlantic Ocean",
            "Indian Ocean",
            "Arctic Ocean",
        ]
    ),
}

# ---------------------------------------------------------------------
# 3. Read and normalize column names
# ---------------------------------------------------------------------
df = pd.read_csv(in_path)
df.columns = [c.strip().upper() for c in df.columns]
df.rename(columns={"GLC2000": "GLC2000", "CARBON_VALUE": "CARBON_VALUE"}, inplace=True)

# ---------------------------------------------------------------------
# 4. Normalize REGION using mapping
# ---------------------------------------------------------------------
df["REGION"] = df["REGION"].map(region_map).fillna(df["REGION"])

# ---------------------------------------------------------------------
# 5. Normalize FRONTIER values
# ---------------------------------------------------------------------
df["FRONTIER"] = (
    df["FRONTIER"]
    .astype(str)
    .replace({"Either": "0;1", "Non-frontier": "0", "Frontier": "1"})
)

# ---------------------------------------------------------------------
# 6. Define full GEZ_TERM list for empty entries (fallback only)
# ---------------------------------------------------------------------
all_gez_terms = [
    "Boreal coniferous forest",
    "Boreal mountain system",
    "Boreal tundra woodland",
    "Polar",
    "Subtropical desert",
    "Subtropical dry forest",
    "Subtropical humid forest",
    "Subtropical mountain system",
    "Subtropical steppe",
    "Temperate continental forest",
    "Temperate desert",
    "Temperate mountain system",
    "Temperate oceanic forest",
    "Temperate steppe",
    "Tropical desert",
    "Tropical dry forest",
    "Tropical moist deciduous forest",
    "Tropical mountain system",
    "Tropical rainforest",
    "Tropical shrubland",
    "Water",
    "No data",
]
df["GEZ_TERM"] = df["GEZ_TERM"].fillna(";".join(all_gez_terms))

# ---------------------------------------------------------------------
# 7. Handle global single-value classes separately
# ---------------------------------------------------------------------
global_mask = df["GLC2000"].str.startswith(("16:", "19:", "20 - 23:"))

df_global = df.loc[global_mask].copy()
df_global["REGION"] = "Global"
df_global["GEZ_TERM"] = "Global"
df_global["FRONTIER"] = "0;1"

df_rest = df.loc[~global_mask].copy()

# ---------------------------------------------------------------------
# 8. Split and explode only non-global rows
# ---------------------------------------------------------------------
df_rest = (
    df_rest.assign(
        REGION=df_rest["REGION"].astype(str).str.split(";"),
        FRONTIER=df_rest["FRONTIER"].astype(str).str.split(";"),
        GEZ_TERM=df_rest["GEZ_TERM"].astype(str).str.split(";"),
    )
    .explode("REGION")
    .explode("FRONTIER")
    .explode("GEZ_TERM")
)

# ---------------------------------------------------------------------
# 9. Combine back and clean
# ---------------------------------------------------------------------
df_final = pd.concat([df_rest, df_global], ignore_index=True)

df_final["REGION"] = df_final["REGION"].str.strip()
df_final["GEZ_TERM"] = df_final["GEZ_TERM"].str.strip()
df_final["FRONTIER"] = (
    pd.to_numeric(df_final["FRONTIER"], errors="coerce").fillna(0).astype(int)
)

# ---------------------------------------------------------------------
# 10. Save exploded table
# ---------------------------------------------------------------------
out_path.parent.mkdir(parents=True, exist_ok=True)
df_final.to_csv(out_path, index=False)
print(f"Exploded lookup written to: {out_path}")
print(f"Rows: {len(df_final):,} | Columns: {len(df_final.columns)}")
