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
# 6. Define full GEZ_TERM list for empty entries (optional fallback)
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

# Fill missing GEZ_TERM with explicit semicolon-joined list (so we can explode)
df["GEZ_TERM"] = df["GEZ_TERM"].fillna(";".join(all_gez_terms))

# ---------------------------------------------------------------------
# 7. Split multi-value fields and explode
# ---------------------------------------------------------------------
df = (
    df.assign(
        REGION=df["REGION"].astype(str).str.split(";"),
        FRONTIER=df["FRONTIER"].astype(str).str.split(";"),
        GEZ_TERM=df["GEZ_TERM"].astype(str).str.split(";"),
    )
    .explode("REGION")
    .explode("FRONTIER")
    .explode("GEZ_TERM")
)

# ---------------------------------------------------------------------
# 8. Clean up whitespace and data types
# ---------------------------------------------------------------------
df["REGION"] = df["REGION"].str.strip()
df["GEZ_TERM"] = df["GEZ_TERM"].str.strip()
df["FRONTIER"] = pd.to_numeric(df["FRONTIER"], errors="coerce").fillna(0).astype(int)

# ---------------------------------------------------------------------
# 9. Save exploded table
# ---------------------------------------------------------------------
out_path.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(out_path, index=False)
print(f"Exploded lookup written to: {out_path}")
print(f"Rows: {len(df):,} | Columns: {len(df.columns)}")
