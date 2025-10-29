import pandas as pd
from pathlib import Path

# Define file paths
rg_path = Path("~/testdata/uga/lookup/rg_lookup_exploded.csv").expanduser()
overlay_path = Path("~/testdata/uga/lookup/uga_overlay_counts.csv").expanduser()

# Read files
rg = pd.read_csv(rg_path)
overlay = pd.read_csv(overlay_path)

# Compare GEZ_TERM coverage
diff = sorted(set(overlay["GEZ_TERM"].unique()) - set(rg["GEZ_TERM"].unique()))
print("GEZ_TERM present in overlay but missing in RG lookup:")
print(diff)
